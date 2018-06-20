package org.codefeedr.core.library.internal.kafka.source

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.core.library.internal.kafka.OffsetUtils
import org.codefeedr.core.library.metastore.{SubjectNode, SynchronizationState}
import org.codefeedr.model.zookeeper.{Consumer, EpochCollection, Partition, QuerySource}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, blocking}
import scala.concurrent.duration._
import resource._

/**
  * Class responsible for observing and modifying zookeeper state and notifying the kafkasource of events/state changes
  * @param kafkaSource the kafkaSource this class manages
  */
class KafkaSourceManager(kafkaSource: GenericKafkaSource,
                         subjectNode: SubjectNode,
                         sourceUuid: String,
                         instanceUuid: String)
    extends LazyLogging {

  private val sourceNode = subjectNode.getSources().getChild(sourceUuid)
  private val consumerNode = sourceNode.getConsumers().getChild(instanceUuid)
  private val syncStateNode = consumerNode.getSyncState()
  private lazy val sourceEpochCollections = sourceNode.getEpochs()
  private lazy val subjectEpochs = subjectNode.getEpochs()
  private lazy val kafkaSourceEpochState = new KafkaSourceEpochState(subjectNode, sourceNode)

  lazy val cancel: Future[Unit] = subjectNode.awaitClose()

  /**
    * Called from the kafkaSource when a run is initialized
    */
  def initializeRun(): Unit = {
    //Create self on zookeeper
    val initialConsumer = Consumer(instanceUuid, null, System.currentTimeMillis())
    //Update zookeeper state blocking, because the source cannot start until the proper zookeeper state has been configured
    sourceNode.createSync(QuerySource(sourceUuid))
    consumerNode.createSync(initialConsumer)
  }

  /**
    * Notify the zookeeper state this source is shutting down
    */
  def finalizeRun(): Unit = {
    //TODO: Also make this syncrhonous
    Await.result(consumerNode.setState(false), 5.seconds)
  }

  /**
    * Notify the zookeeper state this source has started catching up
    * @return
    */
  def startedCatchingUp(): Unit = {
    syncStateNode.setDataSync(SynchronizationState(KafkaSourceState.CatchingUp))
  }

  /**
    * Notify the manager that the consumer has aborted synchronization
    */
  def notifyAbort(): Unit = notifyAggregateStateSync(KafkaSourceState.UnSynchronized)

  /**
    * Notify the manager that the consumer is catched up
    * If all consumers are catched up, the entire source will me marked as catched up (and ready to synchronize)
    */
  def notifyCatchedUp(): Unit = notifyAggregateStateSync(KafkaSourceState.Ready)

  /**
    * Notify the manager that the consumer is now running snchronized
    * If all consumers are running synchronized, the entire source will be synchronized
    */
  def notifySynchronized(): Unit = notifyAggregateStateSync(KafkaSourceState.Synchronized)

  /** Internal function, handling the aggregated state transition between */
  private def notifyAggregateStateSync(state: KafkaSourceState.Value): Unit = {
    syncStateNode.setDataSync(SynchronizationState(state))
    val lock = Await.result(sourceNode.writeLock(), 1.second)
    managed(lock) acquireAndGet { _ =>
      if (sourceNode.getSyncState().getDataSync().get.state != state)
        if (allSourcesInStateSync(state)) {
          logger.debug(s"All sources on ${subjectNode.name} are on state $state")
          sourceNode.getSyncState().setData(SynchronizationState(state))
        }
    }
  }

  /**
    * Notify the manager that the consumer has started on the given epoch
    * Updates the latest epoch id
    */
  def notifyStartedOnEpochSync(epoch: Long): Unit = {
    val latestSource = sourceEpochCollections.getLatestEpochIdSync()
    //If the latest epoch is higher than the present epoch, overwrite the epoch
    if (epoch > latestSource) {
      val lock = Await.result(sourceEpochCollections.writeLock(), 1.second)
      managed(lock) acquireAndGet { _ =>
        sourceEpochCollections.setDataSync(EpochCollection(epoch))
      }
    }
  }

  /** Checks if all sources are in the catched up state **/
  private def allSourcesInStateSync(state: KafkaSourceState.Value): Boolean =
    sourceNode
      .getConsumers()
      .getChildrenSync()
      .map(o => o.getSyncState().getDataSync().get)
      .forall(o => o.state == state)

  /** Checks if the given offset is considered to be catched up with the source
    * Offsets are considered catched up if all offsets are past the second last epoch of the source
    * @param offset the offsets to check
    */
  def isCatchedUpSync(offset: Map[Int, Long]): Boolean = {
    val comparedEpoch = subjectEpochs.getLatestEpochIdSync - 1
    if (comparedEpoch < 0) {
      //If compared epoch is in the history, we just consider to be "up to date"
      true
    } else {
      val comparison = getEpochOffsetsSync(comparedEpoch).map(o => o.nr -> o.offset).toMap
      OffsetUtils.HigherOrEqual(offset, comparison)
    }
  }

  /**
    * Obtains offsets for the subscribed subject of the given epoch
    * If -1 is passed, obtains the current latest offsets
    */
  def getEpochOffsetsSync(epoch: Long): Iterable[Partition] = {
    logger.debug(s"Obtaining offsets for epoch $epoch")
    if (epoch == -1) {
      throw new Exception(
        s"Attempting to obtain endoffsets for epoch -1 in source of ${subjectNode.name}. Did you run the job with checkpointing enabled?")
    } else {
      subjectEpochs.getChild(epoch).getDataSync().get.partitions
    }
  }

  def getLatestSubjectEpochSync: Long = subjectEpochs.getLatestEpochIdSync

  /**
    * Obtain the partitions belonging to the passed synchronized epcoh
    * @param epoch epoch to synchronize with
    * @return the collection of partitions
    */
  def nextSourceEpoch(epoch: Long): Iterable[Partition] =
    kafkaSourceEpochState.nextSourceEpochSync(epoch).partitions

}
