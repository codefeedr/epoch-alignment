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

import java.util
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.runtime.state.{
  CheckpointListener,
  FunctionInitializationContext,
  FunctionSnapshotContext
}
import org.apache.flink.streaming.api.checkpoint.{CheckpointedFunction, ListCheckpointed}
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.types.Row
import org.apache.kafka.clients.consumer.{OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition
import org.codefeedr.core.library.internal.kafka.meta.{PartitionOffset, TopicPartitionOffsets}
import org.codefeedr.core.library.LibraryServices
import org.codefeedr.core.library.internal.kafka.OffsetUtils
import org.codefeedr.core.library.metastore.{ConsumerNode, QuerySourceNode, SubjectNode}
import org.codefeedr.model.zookeeper.{Consumer, QuerySource}
import org.codefeedr.model.{RecordSourceTrail, SubjectType, TrailedRecord}
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise, blocking}
import scala.util.Try

/**
  * Use a single thread to perform all polling operations on kafka
  * Note that new objects will still exist per distributed environment
  *
  * Currently this source is only used as base class for RowSource
  * This source does not support TimestampAssigners yet
  *
  * Because this class needs to be serializable and the LibraryServices are not, no dependency injection structure can be used here :(
  * Created by Niels on 18/07/2017.
  */
abstract class KafkaSource[T](subjectNode: SubjectNode)
    extends RichSourceFunction[T]
    with ResultTypeQueryable[T]
    with CheckpointedFunction
    with CheckpointListener
    //Internal services
    with LazyLogging
    with Serializable
    with LibraryServices {

  @transient protected lazy val consumer = {
    val kafkaConsumer = KafkaConsumerFactory.create[RecordSourceTrail, Row](instanceUuid.toString)
    kafkaConsumer.subscribe(Iterable(topic).asJavaCollection)
    logger.debug(
      s"Source $instanceUuid of consumer $sourceUuid subscribed on topic $topic as group $instanceUuid")
    new KafkaSourceConsumer[T](s"Consumer ${getLabel}", kafkaConsumer, mapToT)
  }

  @transient private lazy val topic = s"${subjectType.name}_${subjectType.uuid}"
  @transient private[kafka] lazy val instanceUuid = UUID.randomUUID().toString
  @transient private lazy val closePromise: Promise[Unit] = Promise[Unit]()

  //Unique id of the source the instance of this kafka source belongs to
  val sourceUuid: String

  @transient protected lazy val subjectType: SubjectType =
    subjectNode.getDataSync().get

  @transient protected lazy val sourceNode: QuerySourceNode =
    subjectNode
      .getSources()
      .getChild(sourceUuid)

  //Node in zookeeper representing state of the instance of the consumer
  @transient protected lazy val consumerNode: ConsumerNode =
    sourceNode
      .getConsumers()
      .getChild(instanceUuid)

  //Running state
  @transient
  @volatile private var state: KafkaSourceState.Value = KafkaSourceState.UnSynchronized
  //Node in zookeeper representing state of the subject this consumer is subscribed on
  @volatile private[kafka] var running = true
  @volatile private[kafka] var intitialized = false

  //CheckpointId after which the source should start shutting down.
  @volatile private[kafka] var finalCheckpointId: Long = Long.MaxValue
  @volatile private[kafka] var finalSourceEpoch: Int = -2
  //TODO: Can we somehow perform this async?
  @transient private[kafka] lazy val finalSourceEpochOffsets = getEpochOffsets(finalSourceEpoch)

  //State of the source. We use the mutable map in operation,
  // and when a snapshot is performed we update the liststate. The liststate contains the offsets of the last comitted checkpoint
  @transient private[kafka] lazy val currentOffsets = consumer.getCurrentOffsets()
  @transient private[kafka] lazy val checkpointOffsets = mutable.Map[Long, Map[Int, Long]]()
  @transient private var listState: ListState[(Int, Long)] = _
  @volatile private var initialized = false

  /**
    * Get a display label for this source
    * @return
    */
  def getLabel(): String = s"KafkaSource ${subjectType.name}($sourceUuid-$instanceUuid)"

  /**
    * Retrieve the current state of the source
    * @return
    */
  def getState(): KafkaSourceState.Value = state

  /**
    * Get a readable print of the given partitions
    * @param partitionOffsets
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
    finalSourceEpoch =
      Await.result(subjectNode.getEpochs().getLatestEpochId(), Duration(1, SECONDS))
    logger.debug(
      s"Cancelling ${getLabel()} after final source epoch ${finalSourceEpoch} has been reached ${finalSourceEpochOffsets}.")
  }

  def readableOffsets(offsetMap: Map[TopicPartition, Long]): String = {
    offsetMap
      .map(tpo => s"(${tpo._1.topic()}_${tpo._1.partition()} -> ${tpo._2})")
      .mkString("\r\n")
  }

  //Called when restoring the state
  override def initializeState(context: FunctionInitializationContext): Unit = {
    logger.info(s"Initializing state of ${getLabel()}")
    if (!getRuntimeContext().asInstanceOf[StreamingRuntimeContext].isCheckpointingEnabled) {
      logger.error(
        "Started a custom source without checkpointing enabled. The custom source is designed to work with checkpoints only.")
      throw new Error(
        "Started a custom source without checkpointing enabled. The custom source is designed to work with checkpoints only.")
    }
    val descriptor = new ListStateDescriptor[(Int, Long)](
      "collected offsets",
      TypeInformation.of(new TypeHint[(Int, Long)]() {})
    )
    //On restore, we only need to restore the current offsets. We won't need the checkpoint offsets any more
    listState = context.getOperatorStateStore.getListState(descriptor)
    //If the state was nonempty, initialize the offsets with the recieved data
    if (listState.get().asScala.nonEmpty) {
      currentOffsets.clear()
      listState.get().asScala.foreach(o => currentOffsets(o._1) = o._2)
    } else {
      //Otherwise just initialize with the default value
      currentOffsets.values
    }
    initialized = true
  }

  //Called when starting a new checkpoint
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    logger.debug(s"Snapshotting epoch ${context.getCheckpointId} on ${getLabel()}")
    checkpointOffsets(context.getCheckpointId) = currentOffsets.toMap
    cancelIfNeeded(context.getCheckpointId)
    logger.debug(s"Done snapshotting epoch ${context.getCheckpointId} on ${getLabel()}")
  }

  /**
    * Obtains offsets for the subscribed subject of the given epoch
    * If -1 is passed, obtains the current latest offsets
    */
  private def getEpochOffsets(epoch: Int): Map[Int, Long] = {
    logger.debug(s"Obtaining offsets for epoch $epoch")
    if (epoch == -1) {
      consumer.getEndOffsets()
    } else {
      Await
        .result(subjectNode.getEpochs().getChild(s"$finalSourceEpoch").getPartitionData(),
                Duration(1, SECONDS))
        .map(o => o.nr -> o.offset)
        .toMap
    }
  }

  /**
    * Checks if a cancel is required
    */
  private def cancelIfNeeded(currentCheckpoint: Long): Unit = {
    if (finalSourceEpoch > -2) {
      logger.debug(s"Attempting to finish on source epoch $finalSourceEpoch")
      logger.debug(s"Current offsets: ${currentOffsets.toMap} (${getLabel()})")
      logger.debug(s"Final offsets: ${finalSourceEpochOffsets} (${getLabel()})")
      if (OffsetUtils.HigherOrEqual(currentOffsets.toMap, finalSourceEpochOffsets)) {
        logger.debug(s"${getLabel()} is cancelling after checkpoint $currentCheckpoint completed.")
        finalCheckpointId = currentCheckpoint
      }
    }
  }

  /**
    * The offset commit to kafka is done on the complete
    * @param checkpointId
    */
  override def notifyCheckpointComplete(checkpointId: Long): Unit = {
    logger.debug(s"${getLabel()} snapshotting offsets for epoch ${checkpointId}")
    listState.clear()
    //Obtain the offsets for the checkpoint that completed
    checkpointOffsets(checkpointId).foreach(o => listState.add(o._1, o._2))

    //Check if the final checkpoint completed
    if (checkpointId == finalCheckpointId) {
      logger.debug(s"${getLabel()} is stopping, final checkpoint ${checkpointId} completed")
      running = false
    }

    consumer.commit(checkpointOffsets(checkpointId))
  }

  /**
    * Performs all operations needed to start up the consumer
    * Blocks on the creation of zookeeper state
    */
  private[kafka] def initRun(): Unit = {
    if(!initialized) {
      throw new Exception(s"Cannot run ${getLabel()} before calling initialize.")
    }

    //Create self on zookeeper
    val initialConsumer = Consumer(instanceUuid, null, System.currentTimeMillis())

    blocking {
      //Update zookeeper state blocking, because the source cannot start until the proper zookeeper state has been configured
      Await.ready(sourceNode.create(QuerySource(sourceUuid)), Duration(5, SECONDS))
      Await.ready(consumerNode.create(initialConsumer), Duration(5, SECONDS))
    }
    //Call cancel when the subject has closed
    subjectNode.awaitClose().map(_ => cancel())
  }

  /**
    * Finalizes the run
    * Called form the notifyCheckpointComplete, because the cleanup cannot occur until the last checkpoint has been completed
    */
  private[kafka] def finalizeRun(): Unit = {
    //Finally unsubscribe from the library
    logger.debug(s"${getLabel()} performing finalization step")
    blocking {
      Await.ready(consumerNode.setState(false), Duration(5, SECONDS))
    }
    consumer.close()
    //Notify of the closing, to whoever is interested
    closePromise.success()
  }

  def mapToT(record: TrailedRecord): T

  /**
    * @return A future that resolves when the source has been close
    */
  private[kafka] def awaitClose(): Future[Unit] = closePromise.future

  /**
    * Perform a poll on the kafka consumer and collect data on the given method
    * Should be  under checkpoint lock because all of this method depends on the dataConsumer, which is not built for multi-threaded access
    */
  def poll(ctx: SourceFunction.SourceContext[T]): Unit = {
    ctx.getCheckpointLock.synchronized {
      val offsets = consumer.poll(ctx)
      offsets.foreach(o => currentOffsets(o._1) = o._2)
    }
    //HACK: Find some way to perform some operations outside the checkpoint lock
    Thread.sleep(10)
  }

  /**
    * When synchronizing, we must make sure to read all data up to the desired offsets, before releasing the checkpoint lock
    * When the desired offset has been reached, it should no longer pull any data
    * @param ctx context to lock on and emit data to
    * @return
    */
  def synchronizedPoll(ctx: SourceFunction.SourceContext[T],
                       offsets: Map[TopicPartition, Long]): Unit = {}

  /**
    * Called at the start of a synchronized epoch
    * Determines the endOffsets for the current epoch
    */
  def startSynchronizedEpoch(): Unit = {
    subjectNode.getEpochs()
  }

  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
    logger.debug(s"Source ${getLabel()} started running.")
    initRun()
    intitialized = true
    while (running) {
      poll(ctx)
    }
    logger.debug(s"Source reach endOffsets ${getLabel()}")

    //Perform cleanup under checkpoint lock
    ctx.getCheckpointLock.synchronized {
      finalizeRun()
      logger.debug(s"Source ${getLabel()} stopped running.")
    }

  }

  /**
    *
    * @param id
    */
  def awaitCheckpoint(id: Long): Unit = {}

  /**
    * Get typeinformation of the returned type
    * @return
    */
  def getProducedType: TypeInformation[T]
}
