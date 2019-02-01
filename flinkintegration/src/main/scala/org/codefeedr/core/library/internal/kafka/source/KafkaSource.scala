/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.codefeedr.core.library.internal.kafka.source

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.runtime.state.{
  CheckpointListener,
  FunctionInitializationContext,
  FunctionSnapshotContext
}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.apache.kafka.common.TopicPartition
import org.codefeedr.configuration.KafkaConfiguration
import org.codefeedr.core.library.internal.logging.MeasuredCheckpointedFunction
import org.codefeedr.core.library.metastore.sourcecommand.{KafkaSourceCommand, SourceCommand}
import org.codefeedr.core.library.metastore.{JobNode, SubjectNode}
import org.codefeedr.model.SubjectType
import org.codefeedr.util.EventTime
import org.codefeedr.util.EventTime._

import scala.async.Async.{async, await}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag

case class KafkaSourceStateContainer(instanceId: String, offsets: Map[Int, Long])

/**
  * Use a single thread to perform all polling operations on kafka
  * Note that new objects will still exist per distributed environment
  *
  * TODO: Clean up this class
  *
  * Because this class needs to be serializable and the LibraryServices are not, no dependency injection structure can be used here :(
  * Created by Niels on 18/07/2017.
  */
abstract class KafkaSource[TElement: EventTime, TValue: ClassTag, TKey: ClassTag](
    subjectNode: SubjectNode,
    jobNode: JobNode,
    kafkaConfiguration: KafkaConfiguration,
    kafkaConsumerFactory: KafkaConsumerFactory)
//Flink interfaces
    extends RichParallelSourceFunction[TElement]
    with ResultTypeQueryable[TElement]
    with CheckpointedFunction
    with CheckpointListener
    //Internal services
    with GenericKafkaSource
    with MeasuredCheckpointedFunction
    with LazyLogging
    with Serializable
    //
    with KafkaSourceMapper[TElement, TValue, TKey] {

  @transient lazy val getMdcMap = Map(
    "operator" -> getOperatorLabel,
    "parallelIndex" -> parallelIndex.toString
  )

  @transient protected lazy val consumer
    : KafkaSourceConsumer[TElement, TValue, TKey] with KafkaSourceMapper[TElement, TValue, TKey] =
    KafkaSourceConsumer[TElement, TValue, TKey](
      s"Consumer $getLabel",
      sourceUuid,
      topic,
      checkpointingMode.contains(CheckpointingMode.EXACTLY_ONCE))(this)(kafkaConsumerFactory)

  //Unique id of the source the instance of this kafka source elongs to
  val sourceUuid: String

  @transient private lazy val topic =
    kafkaConfiguration.getTopic(subjectType)

  //@transient private lazy val closePromise: Promise[Unit] = Promise[Unit]()
  @transient protected lazy val subjectType: SubjectType =
    Await.result(subjectNode.getData(), 5.seconds).get

  //Manager of this source. This should "cleanly" be done by composition, but we cannot do so because this source is "constructed" by Flink
  @transient private[kafka] var manager: KafkaSourceManager = _

  //Running state
  @volatile private var state: KafkaSourceState.Value = KafkaSourceState.UnSynchronized

  //State transitions in progress
  @volatile private var stateTransition: KafkaSourceStateTransition.Value =
    KafkaSourceStateTransition.None

  //Node in zookeeper representing state of the subject this consumer is subscribed on
  @volatile private[kafka] var running = true
  @volatile private[kafka] var initialized = false
  @volatile private[kafka] var started = false

  //CheckpointId after which the source should start shutting down.
  @volatile private[kafka] var finalCheckpointId: Option[Long] = None
  //Boolean that is true when all parallel sources have agreed on the same final source checkpoint id
  @volatile private[kafka] var finalCheckpointIdVerified: Boolean = false

  @volatile private[kafka] var finalSourceEpoch: Long = -2
  @volatile private[kafka] var synchronizeEpochId: Long = -2

  //TODO: Can we somehow perform this async?
  @transient private[kafka] lazy val finalSourceEpochOffsets =
    Await.result(manager.getEpochOffsets(finalSourceEpoch), 1.seconds)
  @transient private[kafka] var checkpointingMode: Option[CheckpointingMode] = _

  @transient private[kafka] lazy val checkpointOffsets = mutable.Map[Long, Map[Int, Long]]()
  @transient private var checkpointedState: ListState[KafkaSourceStateContainer] = _

  @volatile
  @transient private[kafka] var alignmentOffsets: Map[Int, Long] = _

  @transient private lazy val parallelIndex = getRuntimeContext.getIndexOfThisSubtask
  @transient private var sourceState: Option[KafkaSourceStateContainer] = None

  @transient private var gatheredEvents: Long = 0

  override def getCurrentOffset: Long = gatheredEvents

  /**
    * Get a display label for this source
    * @return
    */
  def getLabel: String = getOperatorLabel
  override def getOperatorLabel: String = s"$getCategoryLabel[$parallelIndex]"

  def getCategoryLabel: String =
    s"KafkaSource ${subjectNode.name}($sourceUuid-${sourceState.map(o => o.instanceId).getOrElse("Uninitialized")})"

  private def getSourceState: KafkaSourceStateContainer = sourceState match {
    case None =>
      throw new IllegalStateException(
        s"Cannot request sink state before initialize state is called")
    case Some(s: KafkaSourceStateContainer) => s
  }

  protected def instanceUuid: String = getSourceState.instanceId

  /**
    * Retrieve the current state of the source
    * @return
    */
  def getState: KafkaSourceState.Value = state

  /**
    * Get a readable print of the given partitions.
    * @param partitionOffsets Map of the partitions and their offset
    * @return
    */
  def getReadablePartitions(partitionOffsets: Map[TopicPartition, Long]): String =
    partitionOffsets
      .map(tp => s"p: ${tp._1.topic()}_${tp._1.partition()}, o: ${tp._2}")
      .mkString(", ")

  /**
    * Cancels this source on the final epoch of the source it is subscribed on
    */
  override def cancel(): Unit = {
    logger.warn(s"Cancel was called. Forcing a shutdown of $getLabel")
    if (!initialized) {
      logger.info(s"Cancel was called before the job was actually initialized in $getLabel")
    }
    running = false
  }

  def cancelAsync(): Future[Unit] = async {
    logger.debug(s"Obtaining final source epoch to cancel $getLabel")
    finalSourceEpoch = await(manager.getLatestSubjectEpoch)
    logger.debug(s"Cancelling $getLabel after final source epoch $finalSourceEpoch.")
  }

  /**
    * Apply a command to perform a (state) transition
    * @param command the command
    */
  def apply(command: SourceCommand): Unit = {
    stateTransition.synchronized {
      logger.info(s"Processing command $command in $getLabel")
      command.command match {
        case KafkaSourceCommand.catchUp => catchUpCommand()
        case KafkaSourceCommand.synchronize => synchronizeCommand(command.context.get)
        case KafkaSourceCommand.abort => abortCommand()
        case _ => throw new Error("Unknown command")
      }
    }
  }

  //Called when restoring the state
  override def initializeState(context: FunctionInitializationContext): Unit = {
    sourceState = None

    logger.info(s"Initializing state of $getLabel")
    if (!getRuntimeContext.asInstanceOf[StreamingRuntimeContext].isCheckpointingEnabled) {
      logger.warn(
        "Started a custom source without checkpointing enabled. The custom source is designed to work with checkpoints only.")
      checkpointingMode = None
      // throw new Error(
      // "Started a custom source without checkpointing enabled. The custom source is designed to work with checkpoints only.")

    } else {
      checkpointingMode = Some(CheckpointingMode.EXACTLY_ONCE)
    }

    val descriptor = new ListStateDescriptor[KafkaSourceStateContainer](
      "collected offsets",
      TypeInformation.of(new TypeHint[KafkaSourceStateContainer]() {})
    )

    //On restore, we only need to restore the current offsets. We won't need the checkpoint offsets any more
    checkpointedState = context.getOperatorStateStore.getListState(descriptor)
    val iterator = checkpointedState.get().asScala
    //If the state was nonempty, initialize the offsets with the received data
    if (iterator.nonEmpty) {
      logger.info(s"Initializing $getLabel source with preconfigured state.")
      iterator.foreach((o: KafkaSourceStateContainer) => {
        if (sourceState.nonEmpty) {
          throw new IllegalStateException(
            s"Operator received states of multiple operators upon restore. This is not allowed!")
        }
        sourceState = Some(o)
        //TODO: currentOffsets.clear()
        //o.offsets.foreach(o => currentOffsets(o._1) = o._2)
      })
    } else {
      //Otherwise just initialize with the default value
      sourceState = Some(KafkaSourceStateContainer(parallelIndex.toString, Map.empty[Int, Long]))
    }

    //This construction would normally be done by composition, but because flink constructs the source we cannot do so here
    if (manager == null) {
      manager = new KafkaSourceManager(this, subjectNode, jobNode, sourceUuid, instanceUuid)
    }

    initialized = true
  }

  //Called when starting a new checkpoint
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    super[MeasuredCheckpointedFunction].snapshotState(context)
    val epochId = context.getCheckpointId
    logger.debug(
      s"Snapshotting epoch $epochId on $getLabel. Current state: $state. Current transition: $stateTransition")

    //Retrieve offsets from the consumer
    checkpointOffsets(epochId) = consumer.getCurrentOffsets

    performTransitions(epochId)
    //Handle state specific code
    logger.debug(
      s"State after transition in epoch $epochId on $getLabel: $state. Transition: $stateTransition")
    state match {
      case KafkaSourceState.CatchingUp => snapshotCatchingUpState(epochId)
      case KafkaSourceState.Ready => snapshotReadyState(epochId)
      case KafkaSourceState.Synchronized => snapshotSynchronizedState(epochId)
      case _ =>
    }

    logger.debug(s"$getLabel snapshotting offsets for epoch $epochId")
    val stateContainer = KafkaSourceStateContainer(instanceUuid, consumer.getCurrentOffsets)
    checkpointedState.update(List(stateContainer).asJava)

    logger.debug(s"Done snapshotting epoch ${context.getCheckpointId} on $getLabel")
  }

  /**
    * Performs eventual state transitions in the source
    * @param epochId id of the current epoch
    */
  private def performTransitions(epochId: Long): Unit = {
    //HACK: Sometimes cancelling on first checkpoint will cause incorrect offsets to be obtained (because no assignment happened yet)
    if (epochId > 1) {
      cancelIfNeeded(epochId)
    }

    if (epochId == synchronizeEpochId) {
      if (state == KafkaSourceState.Ready) {
        transitionToSynchronized()
      } else {
        logger.warn(s"Source reached synchronize epoch, but state was $state in source $getLabel")
      }
    }

    //TODO: Should we synchronize here? Technically it is required, but with current usage conflicts cannot occur.
    //If this state transition logic becomes more complex, we should synchronize it with the write operations to stateTransition
    stateTransition match {
      case KafkaSourceStateTransition.Aborting =>
        stateTransition = KafkaSourceStateTransition.None
        if (state != KafkaSourceState.UnSynchronized) {
          manager.notifyAbort()
          state = KafkaSourceState.UnSynchronized
        }
      case KafkaSourceStateTransition.CatchUp =>
        logger.debug("Handling catching up command")
        stateTransition = KafkaSourceStateTransition.None
        state = KafkaSourceState.CatchingUp
        manager.startedCatchingUp()
      case _ =>
    }
  }

  private def snapshotCatchingUpState(epochId: Long): Unit =
    Await.result(
      async {
        if (await(manager.isCatchedUp(consumer.getCurrentOffsets))) {
          state = KafkaSourceState.Ready
          logger.info(s"Transitioned to ready state in $getLabel")
          manager.notifyStartedOnEpoch(epochId)
          manager.notifyCatchedUp()
          alignmentOffsets =
            await(manager.getEpochOffsets(epochId)).map(o => o.nr -> o.offset).toMap
        }
      },
      1.seconds
    )

  /** If th source is in "ready" state, obtain the maximum partition to read up to. We must not read ahead of the latest known epoch*/
  private def snapshotReadyState(epochId: Long): Unit = {
    Await.result(
      for {
        _ <- manager.notifyStartedOnEpoch(epochId)
        _ <- async {
          val epoch = await(manager.getLatestSubjectEpoch)
          alignmentOffsets = await(manager.getEpochOffsets(epoch)).map(o => o.nr -> o.offset).toMap

        }

      } yield Unit,
      5.seconds
    )
  }

  /** If the source is in "synchronized" state, call a specific method on the manager to obtain offsets to synchronize on */
  private def snapshotSynchronizedState(epochId: Long): Unit = {
    Await.result(
      for {
        _ <- manager.notifyStartedOnEpoch(epochId)
        _ <- async {
          alignmentOffsets =
            await(manager.nextSourceEpoch(epochId)).map(o => o.nr -> o.offset).toMap
        }
      } yield Unit,
      5.seconds
    )
  }

  /** Hanlde the state transition to synchronized */
  private def transitionToSynchronized(): Unit = {
    logger.debug(s"Sou  rce is now running synchronized in $getLabel")
    state = KafkaSourceState.Synchronized
    manager.notifySynchronized()
  }

  /**
    * Checks if a cancel is required
    */
  private def cancelIfNeeded(currentCheckpoint: Long): Unit = {
    if (finalSourceEpoch > -2 && finalCheckpointId.isEmpty) {
      logger.debug(s"Attempting to finish on source epoch $finalSourceEpoch")
      logger.debug(s"Current offsets: ${consumer.getCurrentOffsets} ($getLabel)")
      logger.debug(s"Final offsets: $finalSourceEpochOffsets ($getLabel)")

      consumer.higherOrEqual(finalSourceEpochOffsets.map(o => o.nr -> o.offset).toMap) match {
        case Some(true) =>
          logger.debug(s"$getLabel is cancelling after checkpoint $currentCheckpoint completed.")
          finalCheckpointId = Some(currentCheckpoint)
          manager.sourceIsDone(currentCheckpoint)
        case Some(false) =>
          logger.debug(s"$getLabel is not cancelling because it has not reached end offsets")
        case None =>
          logger.debug(
            s"$getLabel is not cancelling because the consumer has not yet been assigned")
      }
    }
  }

  /**
    * The offset commit to kafka is done on the complete
    * @param checkpointId id of the the checkpoint to notify the completion of
    */
  override def notifyCheckpointComplete(checkpointId: Long): Unit = {
    logger.debug(
      s"$getLabel got checkpoint completion notification for checkpoint $checkpointId. Final checkpoint: $finalCheckpointId")

    if (finalCheckpointId.nonEmpty && !finalCheckpointIdVerified) {
      finalCheckpointIdVerified = manager.getVerifiedFinalCheckpoint.nonEmpty
      if (finalCheckpointIdVerified) {
        logger.debug(s"Setting final checkpoint to verified in $getLabel")
      } else {
        logger.debug(s"Cannot set final checkpoint to verified yet in $getLabel")
      }
    }

    //Check if the final checkpoint completed
    if (finalCheckpointId.nonEmpty && finalCheckpointIdVerified && checkpointId >= finalCheckpointId.get) {
      logger.debug(s"$getLabel is stopping, final checkpoint $checkpointId completed")
      running = false
    }

    consumer.commit(checkpointOffsets(checkpointId))
  }

  /**
    * Init run operation. Is called before starting on the runCycle
    * Performs all operations needed to start up the consumer
    * Blocks on the creation of zookeeper state*/
  private[kafka] def initRun(): Unit = {
    if (!initialized) {
      throw new Exception(s"Cannot run $getLabel before calling initialize.")
    }
    manager.initializeRun()
    manager.cancel.onComplete(_ => cancelAsync())

    logger.debug(s"Source $getLabel started running.")
    started = true
  }

  /**
    * Finalizes the run
    * Called form the notifyCheckpointComplete, because the cleanup cannot occur until the last checkpoint has been completed
    */
  private[kafka] def finalizeRun(): Unit = {
    logger.debug(s"$getLabel performing finalization step")
    //First close the consumer
    consumer.close()
    manager.finalizeRun() //Notify manager for distributed state update
    //Notify of the closing, to whoever is interested
    //closePromise.success()
  }

  def transform(value: TValue, key: TKey): TElement

  /**
    * @return A future that resolves when the source has been close
    */
  //private[kafka] def awaitClose(): Future[Unit] = closePromise.future

  /**
    * Perform a poll on the kafka consumer and collect data on the given method
    * Performs poll on kafka, buffering to a buffer. Then collects all data under checkpointlock
    */
  def poll(ctx: SourceFunction.SourceContext[TElement]): Unit = {
    val queue = new mutable.Queue[TElement]

    if (state == KafkaSourceState.Ready) {
      //If ready, make sure not to poll past the latest known offsets to zookeeper
      logger.debug(s"Polling with alignment offset $alignmentOffsets")
      consumer.poll(queue += _, alignmentOffsets)
    } else {
      //Otherwise, just poll
      consumer.poll(queue += _)
    }

    ctx.getCheckpointLock.synchronized {
      while (queue.nonEmpty) {
        val element = queue.dequeue()
        val eventTime = element.getEventTime
        onEvent(eventTime)
        eventTime match {
          case Some(e) =>
            ctx.collectWithTimestamp(element, e)
          case None => ctx.collect(element)

        }
        onEvent(element.getEventTime)
        gatheredEvents += 1

      }
      //Need to update the state in the offset map
      consumer.updateOffsetState()
    }
  }

  /**
    * Perform a synchronized poll, that runs and does not leave the checkpoint lock until the desired offset was reached
    * @param ctx the context to perform the synchronized poll on
    */
  def pollSynchronized(ctx: SourceFunction.SourceContext[TElement]): Unit = {
    logger.trace(s"Currently performing synchronized poll in $getLabel")
    //Do not lock if already reached the offsets

    if (!consumer.higherOrEqual(alignmentOffsets).getOrElse(false)) {

      //Stay within lock until desired offset was reached
      ctx.getCheckpointLock.synchronized {
        while (!consumer.higherOrEqual(alignmentOffsets).getOrElse(false)) {
          logger.debug(
            s"Perfoming synchronized poll loop in $getLabel.\r\n Offsets: ${consumer.getCurrentOffsets}.\r\n Desired: $alignmentOffsets")
          consumer.poll(
            element => {
              val eventTime = element.getEventTime
              onEvent(eventTime)
              eventTime match {
                case Some(e) =>
                  ctx.collectWithTimestamp(element, e)
                case None => ctx.collect(element)
              }
            },
            alignmentOffsets
          )
          consumer.updateOffsetState()
        }
      }
      logger.debug(
        s"Done performing synchronized poll in $getLabel. Offsets are now ${consumer.getCurrentOffsets} with desired offsets $alignmentOffsets")
    }

  }

  /**
    * Performs a single run cycle
    */
  def doCycle(ctx: SourceFunction.SourceContext[TElement]): Unit = {
    if (state == KafkaSourceState.Synchronized) {
      pollSynchronized(ctx)
    } else {
      poll(ctx)
    }

    //HACK: Workaround to support running the source without checkpoints enabled.
    // Currently just performs a checkpoint every loop. Need a proper solution for this!
    if (checkpointingMode.isEmpty) {
      fakeCheckpoint()
    }
  }

  /**
    * Called at the start of a synchronized epoch
    * Determines the endOffsets for the current epoch
    */
  def startSynchronizedEpoch(): Unit = {
    subjectNode.getEpochs()
  }

  override def run(ctx: SourceFunction.SourceContext[TElement]): Unit = {
    ctx.getCheckpointLock.synchronized {
      initRun()
    }

    while (running) {
      doCycle(ctx)
    }

    logger.debug(s"Source reach endOffsets $getLabel")

    logger.debug(s"Source done waiting for eventual other sources to complete  $getLabel")

    //Perform cleanup under checkpoint lock
    ctx.getCheckpointLock.synchronized {
      finalizeRun()
      logger.debug(s"Source $getLabel stopped running.")
    }

  }

  private var increment: Int = 0

  /**
    * Hacky way to perform a fake checkpoint, because the current proof of concept implementation only supprots running in checkpointed mode
    */
  private def fakeCheckpoint(): Unit = {
    logger.info(s"Performing fake checkpoint in $getLabel")
    increment += 1
    val context = new FunctionSnapshotContext {
      override def getCheckpointId: Long = increment
      override def getCheckpointTimestamp: Long = 0
    }
    //Snapshot the current state
    snapshotState(context)
    //And directly complete checkpoint
    notifyCheckpointComplete(increment)
  }

  /**
    * Get typeinformation of the returned type
    * @return
    */
  def getProducedType: TypeInformation[TElement]

  /*
    Below here are the handlers for commands
   */

  private def catchUpCommand(): Unit = {
    assertNoTransition()
    state match {
      case KafkaSourceState.UnSynchronized =>
        stateTransition = KafkaSourceStateTransition.CatchUp
      case _ =>
        val msg = s"Invalid state transition from $state to catchingUp"
        logger.error(msg)
        throw new Error(msg)
    }
  }

  private def synchronizeCommand(context: String): Unit = {
    assertNoTransition()
    state match {
      case KafkaSourceState.Ready =>
        synchronizeEpochId = context.toLong
        logger.debug(s"Synchronizing on epoch $synchronizeEpochId in $getLabel")
      case _ =>
        val msg = s"Invalid state transition from $state to synchronized"
        logger.error(msg)
        throw new Error(msg)
    }
  }

  private def abortCommand(): Unit = {
    logger.info(s"Aborting synchronization in $getLabel. \r\nWas in state: $state")
    stateTransition = KafkaSourceStateTransition.Aborting
  }

  /**
    * Asserts there is no state transition in progress
    * Throws an exception if there is
    */
  private def assertNoTransition(): Unit = {
    if (stateTransition != KafkaSourceStateTransition.None) {
      val msg =
        s"Cannot perform another state transition while state transition to $stateTransition is in progress"
      logger.error(msg)
      throw new Error(msg)
    }
  }

  def readableOffsets(offsetMap: Map[TopicPartition, Long]): String = {
    offsetMap
      .map(tpo => s"(${tpo._1.topic()}_${tpo._1.partition()} -> ${tpo._2})")
      .mkString("\r\n")
  }

  override def close(): Unit = {
    subjectNode.closeConnection()
    super.close()
  }
}
