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

package org.codefeedr.Core.Library.Internal.Kafka.Source

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.runtime.state.{CheckpointListener, FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.types.Row
import org.codefeedr.Core.Library.Internal.Kafka.Meta.{PartitionOffset, TopicPartitionOffsets}
import org.codefeedr.Core.Library.LibraryServices
import org.codefeedr.Core.Library.Metastore.{ConsumerNode, QuerySourceNode, SubjectNode}
import org.codefeedr.Model.Zookeeper.{Consumer, QuerySource}
import org.codefeedr.Model.{RecordSourceTrail, SubjectType, TrailedRecord}

import scala.collection.JavaConverters._
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
  @transient private[Kafka] lazy val instanceUuid = UUID.randomUUID().toString
  //Make this configurable?
  @transient private lazy val RefreshTime = 100

  @transient private lazy val kafkaLatency = 1000
  @transient private lazy val ClosePromise: Promise[Unit] = Promise[Unit]()

  val sourceUuid: String


  @transient protected lazy val subjectNode: SubjectNode =
    subjectLibrary.GetSubject(subjectType.name)

  @transient protected lazy  val sourceNode: QuerySourceNode =
    subjectNode
      .GetSources()
      .GetChild(sourceUuid)

  //Node in zookeeper representing state of the instance of the consumer
  @transient protected lazy val consumerNode: ConsumerNode =
    sourceNode
      .GetConsumers()
      .GetChild(instanceUuid)

  //Node in zookeeper representing state of the subject this consumer is subscribed on

  @transient
  @volatile private[Kafka] var running = true
  @transient
  @volatile private var started = false

  def GetLabel(): String = s"KafkaSource ${subjectType.name}(${sourceUuid}-${instanceUuid})"

  override def cancel(): Unit = {
    logger.debug(s"Source $GetLabel on subject $topic is cancelled")
    if (!started) {
      logger.debug(
        s"Source $GetLabel was cancelled before being started. When started source will still process all events and then terminate.")
    }
    running = false
    if (!started) {
      //If the source never started call finalize manually
      FinalizeRun()
    }
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {}

  override def snapshotState(context: FunctionSnapshotContext): Unit = {}

  override def notifyCheckpointComplete(checkpointId: Long): Unit = {}

  private[Kafka] def InitRun(): Unit = {
    //Create self on zookeeper
    val initialConsumer = Consumer(instanceUuid, null, System.currentTimeMillis())

    //Update zookeeper state blocking, because the source cannot start until the proper zookeeper state has been configured
    Await.ready(sourceNode.Create(QuerySource(sourceUuid)), Duration(5, SECONDS))
    Await.ready(consumerNode.Create(initialConsumer), Duration(5, SECONDS))

    started = true

    //Call cancel when the subject has closed
    subjectNode.AwaitClose().map(_ => cancel())
  }

  private[Kafka] def FinalizeRun(): Unit = {
    //Finally unsubscribe from the library
    logger.debug(s"Unsubscribing ${GetLabel()}on subject $topic.")

    Await.ready(consumerNode.SetState(false), Duration(5, SECONDS))
    dataConsumer.close()
    //Notify of the closing, to whoever is interested
    ClosePromise.success()
  }

  def Map(record: TrailedRecord): T

  /**
    * @return A future that resolves when the source has been close
    */
  private[Kafka] def AwaitClose(): Future[Unit] = ClosePromise.future

  def runLocal(collector: T => Unit): Unit = {
    started = true
    if (!running) {
      logger.debug(s"${GetLabel()} already cancelled. Processing events and terminating")
    }
    logger.debug(s"Source ${GetLabel()} started running.")
    InitRun()


    while (running) {
      //TODO: Handle exceptions
      //Do not need to lock, because there will be only a single thread (per partition set) performing this operation
      val future = Poll().map(
        o => {

          //Collect data and push along the pipeline
          o.foreach(o2 => {
            val mapped = Map(o2)
            logger.debug(s"Got data: $mapped")
            collector(mapped)
          })
          //Obtain offsets, and update zookeeper state
          val offsets = currentOffset()

          //TODO: Implement asynchronous commits
        //  dataConsumer.commitSync()
        }
      )
      Await.ready(future, 5000 millis)
    }


    //TODO: This should be done by closing after offsets have been reached, instead of immediately after zookeeper trigger
    Thread.sleep(1000)
    val future2 = Poll().map(o => o.foreach(o2 => collector(Map(o2))))
    Await.ready(future2, 5000 millis)

    logger.debug(s"Source ${GetLabel()} stopped running.")

    FinalizeRun()
  }


  /**
    * Retrieve the current offsets
    * @return
    */
  def currentOffset(): TopicPartitionOffsets =
    TopicPartitionOffsets(
      topic,
      dataConsumer
        .assignment()
        .asScala
        .map(o => PartitionOffset(o.partition(), dataConsumer.position(o)))
        .toList
    )

  /**
    * Perform a poll on the kafka consumer
    * @return
    */

  def Poll(): Future[List[TrailedRecord]] = {
    val thread = new KafkaConsumerThread(dataConsumer, GetLabel())
    Future {
      thread.run()
      thread.GetData()
    }
  }


  override def run(ctx: SourceFunction.SourceContext[T]): Unit = runLocal(ctx.collect)

  /**
    * Get typeinformation of the returned type
    * @return
    */
  def getProducedType: TypeInformation[T]
}
