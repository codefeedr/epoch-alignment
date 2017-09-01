/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.codefeedr.Core.Library.Internal.Kafka

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.codefeedr.Core.Library.SubjectLibrary
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
  * Created by Niels on 18/07/2017.
  */
class KafkaSource(subjectType: SubjectType)
    extends RichSourceFunction[TrailedRecord]
    with LazyLogging {

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
  @transient private lazy val ClosePromise: Promise[Unit] = Promise[Unit]()

  @transient
  @volatile private[Kafka] var running = true
  @transient @volatile private var started = false

  override def cancel(): Unit = {
    logger.debug(s"Source $uuid on subject $topic is cancelled")
    running = false
    if(!started) {
      //If the source never started call finalize manually
      FinalizeRun()
    }

  }

  private[Kafka] def InitRun(): Unit = {
    Await.ready(SubjectLibrary.RegisterSource(subjectType.name, uuid.toString),
                Duration(120, SECONDS))
    //Make sure to cancel when the subject closes
    SubjectLibrary.AwaitClose(subjectType.name).onComplete(_ => cancel())
  }

  private[Kafka] def FinalizeRun(): Unit = {
    //Finally unsubscribe from the library
    logger.debug(s"Unsubscribing source $uuid on subject $topic.")
    Await.ready(SubjectLibrary.UnRegisterSource(subjectType.name, uuid.toString),
                Duration(120, SECONDS))
    //Notify of the closing
    ClosePromise.success()
  }

  /**
    * @return A future that resolves when the source has been close
    */
  private[Kafka] def AwaitClose(): Future[Unit] = ClosePromise.future

  override def run(ctx: SourceFunction.SourceContext[TrailedRecord]): Unit = {
    started = true
    if(!running) {
      throw new Exception(s"$uuid already cancelled. Cannot start a cancelled source")
    }
    logger.debug(s"Source $uuid started running.")
    InitRun()

    val refreshTask = new Runnable {
      override def run(): Unit = {
        while (running) {
          delay(RefreshTime)
        }
        //After cancel poll keep polling until all data has been received
        while (Poll()) {
          delay(RefreshTime)
        }
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
            ctx.collect(o)
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
}
