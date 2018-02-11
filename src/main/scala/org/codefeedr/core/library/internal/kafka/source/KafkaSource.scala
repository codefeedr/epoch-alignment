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
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.runtime.state.{
  CheckpointListener,
  FunctionInitializationContext,
  FunctionSnapshotContext
}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.types.Row
import org.apache.kafka.clients.consumer.{OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition
import org.codefeedr.core.library.internal.kafka.meta.{PartitionOffset, TopicPartitionOffsets}
import org.codefeedr.core.library.LibraryServices
import org.codefeedr.core.library.metastore.{ConsumerNode, QuerySourceNode, SubjectNode}
import org.codefeedr.model.zookeeper.{Consumer, QuerySource}
import org.codefeedr.model.{RecordSourceTrail, SubjectType, TrailedRecord}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
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
abstract class KafkaSource[T](subjectType: SubjectType)
    extends RichSourceFunction[T]
    with ResultTypeQueryable[T]
    with CheckpointedFunction
    with CheckpointListener
    //Internal services
    with LazyLogging
    with Serializable
    with LibraryServices {

  @transient private lazy val dataConsumer = {
    val consumer = KafkaConsumerFactory.create[RecordSourceTrail, Row](instanceUuid.toString)
    consumer.subscribe(Iterable(topic).asJavaCollection)
    logger.debug(
      s"Source $instanceUuid of consumer $sourceUuid subscribed on topic $topic as group $instanceUuid")
    consumer
  }

  @transient private lazy val topic = s"${subjectType.name}_${subjectType.uuid}"
  @transient private[kafka] lazy val instanceUuid = UUID.randomUUID().toString
  @transient private lazy val closePromise: Promise[Unit] = Promise[Unit]()

  //Timeout when polling kafka
  @transient private lazy val pollTimeout = 1000

  val sourceUuid: String

  @transient protected lazy val subjectNode: SubjectNode =
    subjectLibrary.getSubject(subjectType.name)

  @transient protected lazy val sourceNode: QuerySourceNode =
    subjectNode
      .getSources()
      .getChild(sourceUuid)

  //Node in zookeeper representing state of the instance of the consumer
  @transient protected lazy val consumerNode: ConsumerNode =
    sourceNode
      .getConsumers()
      .getChild(instanceUuid)

  //Node in zookeeper representing state of the subject this consumer is subscribed on

  @transient
  @volatile private[kafka] var running = true
  @transient
  @volatile private var started = false

  //State of the source
  @transient private var currentOffsets: Map[TopicPartition, Long] = _

  //Contains for each checkpoint in progress the collection of topicPartitions that should be committed
  @transient private lazy val shouldCommit: mutable.Map[Long, Map[TopicPartition, Long]] =
    mutable.Map.empty[Long, Map[TopicPartition, Long]]

  //Get a display label of the current source
  def getLabel(): String = s"KafkaSource ${subjectType.name}($sourceUuid-$instanceUuid)"

  //Get a readable print of the partitions and offset.
  def getReadablePartitions(partitionOffsets: Map[TopicPartition, Long]): String =
    partitionOffsets
      .map(tp => s"p: ${tp._1.topic()}_${tp._1.partition()}, o: ${tp._2}")
      .mkString(", ")

  override def cancel(): Unit = {
    logger.debug(s"Source $getLabel on subject $topic is cancelled")
    if (!started) {
      logger.debug(
        s"Source $getLabel was cancelled before being started. When started source will still process all events and then terminate.")
    }
    running = false
    if (!started) {
      //If the source never started call finalize manually
      finalizeRun()
    }
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {}

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    shouldCommit += context.getCheckpointId -> currentOffsets
  }

  override def notifyCheckpointComplete(checkpointId: Long): Unit = {
    val offsets = shouldCommit.get(checkpointId)
    if (offsets.isEmpty) {
      throw new RuntimeException(
        s"Got a checkpoint completed for unknown checkpoint id ${checkpointId}")
    }
    if (offsets.get == null) {
      logger.debug(s"${getLabel()} ignoring checkpoint $checkpointId because offsets was null")
    } else {
      dataConsumer.commitAsync(
        offsets.get.map(o => (o._1, new OffsetAndMetadata(o._2))).asJava,
        new OffsetCommitCallback {
          override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata],
                                  exception: Exception): Unit = {
            val parsedOffset = offsets.asScala.map(o => (o._1, o._2.offset()))
            logger.debug(
              s"Offsetts ${getReadablePartitions(parsedOffset.toMap)} successfully committed to kafka")
          }
        }
      )
    }
  }

  private[kafka] def initRun(): Unit = {
    //Create self on zookeeper
    val initialConsumer = Consumer(instanceUuid, null, System.currentTimeMillis())

    //Update zookeeper state blocking, because the source cannot start until the proper zookeeper state has been configured
    Await.ready(sourceNode.create(QuerySource(sourceUuid)), Duration(5, SECONDS))
    Await.ready(consumerNode.create(initialConsumer), Duration(5, SECONDS))

    started = true

    //Call cancel when the subject has closed
    subjectNode.awaitClose().map(_ => cancel())
  }

  private[kafka] def finalizeRun(): Unit = {
    //Finally unsubscribe from the library
    logger.debug(s"Unsubscribing ${getLabel}on subject $topic.")

    Await.ready(consumerNode.setState(false), Duration(5, SECONDS))
    dataConsumer.close()
    //Notify of the closing, to whoever is interested
    closePromise.success()
  }

  def mapToT(record: TrailedRecord): T

  /**
    * @return A future that resolves when the source has been close
    */
  private[kafka] def awaitClose(): Future[Unit] = closePromise.future

  /**
    * Retrieves the current offsets that have been pushed along, but have not been committed yet
    * @return
    */
  def getUncommittedOffset(): Map[TopicPartition, Long] =
    dataConsumer
      .assignment()
      .asScala
      .map(o => o -> dataConsumer.position(o))
      .toMap

  /**
    * Perform a poll on the kafka consumer and collect data on the given method
    * Completely under checkpoint lock because all of this method depends on the dataConsumer, which is not built for multi-threaded access
    */
  def poll(ctx: SourceFunction.SourceContext[T]): Unit = ctx.getCheckpointLock.synchronized {
    logger.debug(s"${getLabel} started polling")
    val data = dataConsumer.poll(pollTimeout).iterator().asScala
    logger.debug(s"${getLabel} completed polling")
    data.foreach(o => ctx.collect(mapToT(TrailedRecord(o.value()))))
    currentOffsets = getUncommittedOffset()
    logger.debug(s"Completed ${getLabel()} poll, ${getReadablePartitions(currentOffsets)}")
  }

  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
    started = true
    if (!running) {
      logger.debug(s"${getLabel()} already cancelled. Processing events and terminating")
    }
    logger.debug(s"Source ${getLabel()} started running.")
    initRun()
    while (running) {
      poll(ctx)
    }

    //TODO: This should be done by closing after offsets have been reached, instead of immediately after zookeeper trigger
    Thread.sleep(1000)
    poll(ctx)

    logger.debug(s"Source ${getLabel()} stopped running.")
    finalizeRun()
  }

  /**
    * Get typeinformation of the returned type
    * @return
    */
  def getProducedType: TypeInformation[T]
}
