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

package org.codefeedr.Core.Library.Internal.Kafka

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.codefeedr.Core.Library.{LibraryServices}
import org.codefeedr.Model.{Record, RecordSourceTrail, SubjectType, TrailedRecord}

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

/**
  * Use a single thread to perform all polling operations on kafka
  * Note that new objects will still exist per distributed environment
  */
/**
  * Because this class needs to be serializable and the LibraryServices are not, no dependency injection structure can be used here :(
  * Created by Niels on 18/07/2017.
  */
class KafkaSource(subjectType: SubjectType)
    extends RichSourceFunction[TrailedRecord]
    with LazyLogging
    with Serializable
    with LibraryServices {

  @transient private lazy val dataConsumer = {
    val consumer = KafkaConsumerFactory.create[RecordSourceTrail, Record](uuid.toString)
    consumer.subscribe(Iterable(topic).asJavaCollection)
    logger.debug(s"Source $uuid subscribed on topic $topic as group $uuid")
    consumer
  }
  @transient private lazy val topic = s"${subjectType.name}_${subjectType.uuid}"
  @transient private[Kafka] lazy val uuid = UUID.randomUUID()

  //Make this configurable?
  @transient private lazy val RefreshTime = 100
  @transient private lazy val PollTimeout = 1000
  @transient private lazy val kafkaLatency = 1000
  @transient private lazy val ClosePromise: Promise[Unit] = Promise[Unit]()

  @transient
  @volatile private[Kafka] var running = true
  @transient
  @volatile private var started = false

  override def cancel(): Unit = {
    logger.debug(s"Source $uuid on subject $topic is cancelled")
    if (!started) {
      logger.debug(
        s"Source $uuid was cancelled before being started. When started source will still process all events and then terminate.")
    }
    running = false
    if (!started) {
      //If the source never started call finalize manually
      FinalizeRun()
    }

  }

  private[Kafka] def InitRun(): Unit = {
    Await.ready(subjectLibrary.RegisterSource(subjectType.name, uuid.toString),
                Duration(120, SECONDS))
    //Make sure to cancel when the subject closes
    subjectLibrary.AwaitClose(subjectType.name).onComplete(_ => cancel())
  }

  private[Kafka] def FinalizeRun(): Unit = {
    //Finally unsubscribe from the library
    logger.debug(s"Unsubscribing source $uuid on subject $topic.")
    Await.ready(subjectLibrary.UnRegisterSource(subjectType.name, uuid.toString),
                Duration(120, SECONDS))
    dataConsumer.close()
    //Notify of the closing
    ClosePromise.success()
  }

  /**
    * @return A future that resolves when the source has been close
    */
  private[Kafka] def AwaitClose(): Future[Unit] = ClosePromise.future

  def runLocal(collector: TrailedRecord => Unit): Unit = {
    started = true
    if (!running) {
      logger.debug(s"$uuid already cancelled. Processing events and terminating")
    }
    logger.debug(s"Source $uuid started running.")
    InitRun()

    val refreshTask = new Runnable {
      override def run(): Unit = {
        //Poll at least once
        Poll()
        while (running) {
          Poll()
        }
        //Keep polling until no more data
        while (Poll()) {}
        FinalizeRun()
      }

      //TODO: Use a cleaner delay (for example using akka?)
      def delay(d: Long): Unit = {
        Try(Await.ready(Promise().future, Duration(d, MILLISECONDS)))
      }

      //TODO: Solve this foundRecords variable some cleaner way
      def Poll(): Boolean = {
        logger.debug(s"$uuid Polling")
        var foundRecords = false
        dataConsumer
          .poll(PollTimeout)
          .iterator()
          .asScala
          .map(o => TrailedRecord(o.value(), o.key()))
          .foreach(o => {
            foundRecords = true
            collector(o)
          })
        logger.debug(s"$uuid completed poll")
        foundRecords
      }

    }
    //Maybe refactor this back to just sleeping in the main thread.
    val thread = new Thread(refreshTask)
    thread.setDaemon(true)
    thread.start()
    thread.join()
  }

  override def run(ctx: SourceFunction.SourceContext[TrailedRecord]): Unit = runLocal(ctx.collect)
}
