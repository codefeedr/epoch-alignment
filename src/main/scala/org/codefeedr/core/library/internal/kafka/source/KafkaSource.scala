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
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.java.tuple.Tuple2
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

  @transient
  @volatile private var loopEnded = false

  //State of the source
  @transient private var currentOffsets: Map[TopicPartition, Long] = _
  @transient
  @volatile private var endOffsets: Map[TopicPartition, Long] = _

  //Contains for each checkpoint in progress the collection of topicPartitions that should be committed
  @transient
  @volatile private lazy val shouldCommit: mutable.Queue[(Long, Map[TopicPartition, Long])] =
    mutable.Queue.empty[(Long, Map[TopicPartition, Long])]

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
  }

  /**
    * Returns true if the consumer has not reached the end offset (so must continue polling)
    * Throws an exception if the consumer surpassed the endOffset
    * @return
    */
  def notReachedOffsets(): Boolean = {
    if (endOffsets == null) {
      true
    } else {
      currentOffsets.exists(o => {
        if (endOffsets(o._1) > o._2) {
          true
        } else {
          if (endOffsets(o._1) > o._2) {
            throw new RuntimeException(s"Surpassed end-offset for ${o._1} on ${getLabel()}")
          }
          false
        }
      })
    }
  }

  def readableOffsets(offsetMap: Map[TopicPartition, Long]): String = {
    offsetMap
      .map(tpo => s"(${tpo._1.topic()}_${tpo._1.partition()} -> ${tpo._2})")
      .mkString("\r\n")
  }

  /**
    * Sets the endOffsets, so it is known on which offsets the job ended
    */
  def cancelOnOffsets(): Unit = {
    logger.debug(s"Obtaining endOffsets for ${getLabel()}")
    // <3 converting java to scala (not)
    endOffsets = dataConsumer
      .endOffsets(dataConsumer.assignment())
      .asScala
      .map(o => o._1 -> Long2long(o._2))
      .toMap
    logger.debug(s"obtained endOffsets for ${getLabel()}: \r\n${readableOffsets(endOffsets)}")
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    //context.getOperatorStateStore.getUnionListState(
    // new ListStateDescriptor[Tuple2[KafkaTopicPartition, Long]]("topic-partition-offset", TypeInformation.of(new TypeHint[Tuple2[KafkaTopicPartition, Long]]() {})))
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    logger.debug(s"${getLabel()} snapshotting offsets for epoch ${context.getCheckpointId}")
    shouldCommit += context.getCheckpointId -> currentOffsets
  }

  /**
    * The offset commit to kafka is done on the complete
    * @param checkpointId
    */
  override def notifyCheckpointComplete(checkpointId: Long): Unit = {
    logger.debug(s"${getLabel()} completing epoch ${checkpointId}")
    val cp = shouldCommit.dequeue()

    //this should never happen, but just perform it as validation
    if (cp._1 != checkpointId) {
      throw new RuntimeException(
        s"Got a checkpoint completed for ${checkpointId} while waiting for ${cp._1}")
    }

    val offsets = cp._2
    if (offsets == null) {
      logger.debug(s"${getLabel()} ignoring checkpoint $checkpointId because offsets was null")
    } else {
      if (loopEnded) {
        logger.warn(
          s"Cannot commit offsets for epoch $checkpointId on ${getLabel()}. Consumer already closed")
      } else {
        dataConsumer.commitAsync(
          offsets.map(o => (o._1, new OffsetAndMetadata(o._2))).asJava,
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
  }

  /**
    * Performs all operations needed to start up the consumer
    * Blocks on the creation of zookeeper state
    */
  private[kafka] def initRun(): Unit = {
    //Create self on zookeeper
    val initialConsumer = Consumer(instanceUuid, null, System.currentTimeMillis())

    blocking {
      //Update zookeeper state blocking, because the source cannot start until the proper zookeeper state has been configured
      Await.ready(sourceNode.create(QuerySource(sourceUuid)), Duration(5, SECONDS))
      Await.ready(consumerNode.create(initialConsumer), Duration(5, SECONDS))
    }
    started = true

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
    * Should be  under checkpoint lock because all of this method depends on the dataConsumer, which is not built for multi-threaded access
    */
  def poll(ctx: SourceFunction.SourceContext[T]): Unit = {
    ctx.getCheckpointLock.synchronized {
      logger.debug(s"${getLabel} started polling")
      val data = dataConsumer.poll(pollTimeout).iterator().asScala
      logger.debug(s"${getLabel} completed polling")
      data.foreach(o => ctx.collect(mapToT(TrailedRecord(o.value()))))
      currentOffsets = getUncommittedOffset()
      logger.debug(s"Completed ${getLabel()} poll, ${getReadablePartitions(currentOffsets)}")

      //Retrieve endoffsets if cancelled. Needs to be done within synchronization
      if (!running && endOffsets == null) {
        cancelOnOffsets()
      }
    }
  }

  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
    started = true
    if (!running) {
      logger.debug(s"${getLabel()} already cancelled. Processing events and terminating")
    }
    logger.debug(s"Source ${getLabel()} started running.")
    initRun()
    while (notReachedOffsets()) {
      poll(ctx)

    }
    logger.debug(s"Source reach endOffsets ${getLabel()}")

    //Perform cleanup under checkpoint lock
    ctx.getCheckpointLock.synchronized {
      finalizeRun()
      loopEnded = true
      logger.debug(s"Source ${getLabel()} stopped running.")
    }

  }

  /**
    * Get typeinformation of the returned type
    * @return
    */
  def getProducedType: TypeInformation[T]
}
