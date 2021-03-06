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
package org.codefeedr.core.plugin

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.datastream.{AsyncDataStream => JavaAsyncDataStream}
import org.codefeedr.core.library.internal.{JobComponent, Plugin}
import org.apache.flink.api.scala._
import org.codefeedr.core.library.SubjectFactoryComponent
import org.codefeedr.core.library.internal.kafka.source.KafkaConsumerFactoryComponent
import org.codefeedr.core.library.metastore.SubjectLibraryComponent
import org.codefeedr.plugins.github.clients.GitHubProtocol.{Event, Payload, PushEvent}
import org.codefeedr.plugins.github.input.GitHubSource
import org.codefeedr.plugins.github.operators.GetOrAddPushEvent
import org.json4s.DefaultFormats
import org.codefeedr.plugins.github.clients.EventTimeImpl._

trait EventsJobFactoryComponent {
  this: SubjectLibraryComponent
    with SubjectFactoryComponent
    with KafkaConsumerFactoryComponent
    with JobComponent =>

  def createEventsJob(maxRequests: Int) = new EventsJob(maxRequests)

  import org.codefeedr.util.NoEventTime._
  class EventsJob(maxRequests: Int) extends Job[Event, PushEvent]("events_job") {

    /**
      * Setups a stream for the given environment.
      *
      * @param env the environment to setup the stream on.
      * @return the prepared datastream.
      */
    override def getStream(env: StreamExecutionEnvironment): DataStream[PushEvent] = {
      val source = new GitHubSource(maxRequests)
      val stream =
        env.addSource(source).filter(_.`type` == "PushEvent").map { x =>
          implicit val formats = DefaultFormats
          PushEvent(x.id,
                    x.repo,
                    x.actor,
                    x.org,
                    x.payload.extract[Payload],
                    x.public,
                    x.created_at)
        }

      //work around for not existing RichAsyncFunction in Scala
      val asyncFunction = new GetOrAddPushEvent
      val finalStream =
        JavaAsyncDataStream.unorderedWait(stream.javaStream,
                                          asyncFunction,
                                          10,
                                          TimeUnit.SECONDS,
                                          50)

      new org.apache.flink.streaming.api.scala.DataStream(finalStream)
    }
  }
}
