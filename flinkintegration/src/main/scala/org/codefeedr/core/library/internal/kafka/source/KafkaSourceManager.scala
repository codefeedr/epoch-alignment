package org.codefeedr.core.library.internal.kafka.source

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.core.library.internal.kafka.OffsetUtils
import org.codefeedr.core.library.metastore._
import org.codefeedr.model.zookeeper.{Consumer, EpochCollection, Partition, QuerySource}

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, Future, blocking}

/**
  * Class responsible for observing and modifying zookeeper state and notifying the kafkasource of events/state changes
  * @param kafkaSource the kafkaSource this class manages
  */
class KafkaSourceManager(kafkaSource: GenericKafkaSource,
                         subjectNode: SubjectNode,
                         jobNode: JobNode,
                         sourceUuid: String,
                         instanceUuid: String)
    extends LazyLogging {

  private val sourceCollectionNode = subjectNode.getSources()
  private val sourceNode = sourceCollectionNode.getChild(sourceUuid)

  private val commandNode = sourceNode.getCommandNode()

  private val consumerCollectionNode = sourceNode.getConsumers()
  private val consumerNode = consumerCollectionNode.getChild(instanceUuid)

  private val jobConsumerCollectionNode = jobNode.getJobConsumerCollection()
  private val jobConsumer = jobConsumerCollectionNode.getChild(s"$sourceUuid-$instanceUuid")

  private val syncStateNode = consumerNode.getSyncState()
  private lazy val sourceEpochCollections = sourceNode.getEpochs()
  private lazy val subjectEpochs = subjectNode.getEpochs()
  private lazy val kafkaSourceEpochState = new KafkaSourceEpochState(subjectNode, sourceNode)

  private lazy val timeout = Duration(30, SECONDS)

  lazy val cancel: Future[Unit] = subjectNode.awaitClose()

  private def getLabel = s"SourceManager ${consumerNode.path()}"

  /**
    * Called from the kafkaSource when a run is initialized
    */
  def initializeRun(): Unit = {
    //Create self on zookeeper
    val initialConsumer = Consumer(instanceUuid, null, System.currentTimeMillis())

    //Update zookeeper state blocking, because the source cannot start until the proper zookeeper state has been configured
    Await.result(sourceNode.sync(), timeout)
    if (!Await.result(sourceNode.exists(), timeout)) {
      throw new IllegalStateException(
        s"Source $sourceUuid does not exist on path ${sourceNode.path()}. Did you construct the source via the subjectFactory?")
    }
    Await.result(consumerNode.create(initialConsumer), timeout)

    Await.result(jobConsumer.create(), timeout)
    logger.trace(s"Initialized sourceconsumerstate for $getLabel")

    linkCommandNode()
    logger.debug(s"Linked commandNode for $getLabel")
  }

  private def linkCommandNode(): Unit = {
    commandNode
      .observe()
      .subscribe(o => {
        logger.info(s"Got command $o in $getLabel")
        kafkaSource.apply(o)
      })
  }

  def getState: ConsumerState = {
    Await.result(consumerNode.getState(), timeout).get
  }

  /**
    * Notify the zookeeper state this source is shutting down
    */
  def finalizeRun(): Unit = {
    //Finally unsubscribe from the library
    val state = getState
    Await.result(consumerNode.setState(ConsumerState(open = false)), timeout)
  }

  /**
    * Notify the zookeeper state this source has started catching up
    * @return
    */
  def startedCatchingUp(): Future[Unit] = {
    syncStateNode.setData(SynchronizationState(KafkaSourceState.CatchingUp))
  }

  /**
    * Notify the manager that the consumer has aborted synchronization
    */
  def notifyAbort(): Future[Unit] = notifyAggregateState(KafkaSourceState.UnSynchronized)

  /**
    * Notify the manager that the consumer is catched up
    * If all consumers are catched up, the entire source will me marked as catched up (and ready to synchronize)
    */
  def notifyCatchedUp(): Future[Unit] = notifyAggregateState(KafkaSourceState.Ready)

  /**
    * Notify the manager that the consumer is now running snchronized
    * If all consumers are running synchronized, the entire source will be synchronized
    */
  def notifySynchronized(): Future[Unit] = notifyAggregateState(KafkaSourceState.Synchronized)

  /** Internal function, handling the aggregated state transition between */
  private def notifyAggregateState(state: KafkaSourceState.Value): Future[Unit] = async {
    await(syncStateNode.setData(SynchronizationState(state)))
    await(sourceNode.asyncWriteLock(() =>
      async {
        //await(sourceNode.sync())
        if (await(sourceNode.getSyncState().getData()).get.state != state)
          if (await(allSourcesInState(state))) {
            logger.info(s"All sources on ${subjectNode.name} are on state $state")
            sourceNode.getSyncState().setData(SynchronizationState(state))
          } else {
            logger.info(s"Not all sources on ${subjectNode.name} are on state $state")
          }
    }))
  }

  /**
    * Notify the manager that the consumer has started on the given epoch
    * Updates the latest epoch id
    */
  def notifyStartedOnEpoch(epoch: Long): Future[Unit] = async {
    val latestSource = await(sourceEpochCollections.getLatestEpochId())
    //If the latest epoch is higher than the present epoch, overwrite the epoch
    if (epoch > latestSource) {
      sourceEpochCollections.asyncWriteLock(() => {
        sourceEpochCollections.setData(EpochCollection(epoch))
      })
    }
  }

  /** Checks if all sources are in the catched up state **/
  private def allSourcesInState(state: KafkaSourceState.Value): Future[Boolean] =
    sourceNode
      .getConsumers()
      .getChildren()
      .flatMap(
        o =>
          Future
            .sequence(
              o.map(
                o => o.getSyncState().getData().map(o2 => {
              //    logger.info(s"State in node: ${o.name}: ${o2.get}(${o2.get.state.id}). Comparing with ${state}(${state.id}).")
                  o2.get
                })
              )
            )
            .map(o => o.forall(o => o.state.id == state.id))
      )

  /** Checks if the given offset is considered to be catched up with the source
    * Offsets are considered catched up if all offsets are past the second last epoch of the source
    * @param offset the offsets to check
    */
  def isCatchedUp(offset: Map[Int, Long]): Future[Boolean] = async {
    val comparedEpoch = await(subjectEpochs.getLatestEpochId) - 1
    if (comparedEpoch < 0) {
      //If compared epoch is in the history, we just consider to be "up to date"
      true
    } else {
      val comparison = await(getEpochOffsets(comparedEpoch)).map(o => o.nr -> o.offset).toMap
      logger.debug(s"Catching up $getLabel. Comparing $offset with $comparison for is catched up")
      val result = OffsetUtils.HigherOrEqual(offset, comparison)
      if (result) {
        logger.debug(s"Considered catchedup $getLabel")
      }
      result
    }
  }

  /**
    * Obtains offsets for the subscribed subject of the given epoch
    * If -1 is passed, obtains the current latest offsets
    */
  def getEpochOffsets(epoch: Long): Future[Iterable[Partition]] = async {
    logger.info(s"Obtaining offsets for epoch $epoch")
    if (epoch == -1) {
      throw new Exception(
        s"Attempting to obtain endoffsets for epoch -1 in source of ${subjectNode.name}. Did you run the job with checkpointing enabled?")
    } else {
      //await(subjectEpochs.sync())
      await(subjectEpochs.getChild(epoch).getData()).get.partitions.map(o =>
        Partition(o._1, o._2))
    }
  }

  def getLatestSubjectEpoch: Future[Long] = subjectEpochs.getLatestEpochId

  /**
    * Obtain the partitions belonging to the passed synchronized epcoh
    * @param epoch epoch to synchronize with
    * @return the collection of partitions
    */
  def nextSourceEpoch(epoch: Long): Future[Iterable[Partition]] = async {
    await(kafkaSourceEpochState.nextSourceEpoch(epoch)).partitions.map(o => Partition(o._1, o._2))
  }

  /**
    * Retrieves the final checkpoint id from the zookeeper state, or None if no checkpoint id was agreed on
    * @return the agreed final checkpoint id, or none if no final checkpoint was agreed on
    */
  def getVerifiedFinalCheckpoint: Option[Long] = {
    logger.trace(s"consumer states on ${jobConsumerCollectionNode.path()} are: $getConsumerState")
    val aggregateState = await(jobConsumerCollectionNode.getState())
    logger.trace(s"consumer aggregate state is: $aggregateState")
    aggregateState.finalEpoch
  }

  private def getConsumerState: String = {
    val children = await(jobConsumerCollectionNode.getChildren())
    val states = children.map(o => (o, await(o.getState())))
    s"\r\n${states.map(o => s"${o._1.name}: ${o._2}").mkString("\r\n")}\r\n"
  }

  /**
    *   Notifies the zookeeper state that the current source is done
    * @param currentCheckpoint
    */
  def sourceIsDone(currentCheckpoint: Long): Unit = {
    val lock = await(jobConsumerCollectionNode.writeLock())
    try {
      val aggregateState = await(jobConsumerCollectionNode.getState())
      if (aggregateState.finalEpoch.nonEmpty) {
        logger.info(
          s"Not updating final checkpoint in $getLabel to $currentCheckpoint because final checkpoint ${aggregateState.finalEpoch.get} was already assigned")
      } else {
        logger.info(s"updating final checkpoint in $getLabel to $currentCheckpoint")
        await(jobConsumer.setState(JobConsumerState(Some(currentCheckpoint), open = true)))
      }
    } finally {
      lock.close()
    }
  }

  private def await[T](future: Future[T]): T = {
    Await.result(future, timeout)
  }

}
