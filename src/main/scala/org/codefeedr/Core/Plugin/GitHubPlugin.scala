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

import com.google.gson.{Gson, GsonBuilder, JsonObject}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.datastream.{AsyncDataStream => JavaAsyncDataStream}
import org.apache.flink.streaming.api.functions.async.{AsyncFunction => JavaAsyncFunction}

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
import org.apache.flink.api.scala._
import org.codefeedr.core.clients.GitHub.GitHubProtocol
import org.codefeedr.core.clients.GitHub.GitHubProtocol.{Payload, PushEvent}
import org.codefeedr.core.input.GitHubSource
import org.codefeedr.core.library.SubjectFactory
import org.codefeedr.core.library.internal.{AbstractPlugin, SubjectTypeFactory}
import org.codefeedr.core.operators.GetOrAddPushEvent
import org.codefeedr.model.SubjectType
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.async.Async

class GitHubPlugin[PushEvent: ru.TypeTag: ClassTag](maxRequests: Integer = -1)
    extends AbstractPlugin {

  /**
    * Creates a new SubjectType.
    * @return
    */
  override def createSubjectType(): SubjectType = {
    return SubjectTypeFactory.getSubjectType[PushEvent]

  }

  /**
    * Gets the stream.
    * @param env the environment to prepare.
    * @return the data stream.
    */
  def getStream(env: StreamExecutionEnvironment): DataStream[GitHubProtocol.PushEvent] = {
    val stream =
      env.addSource(new GitHubSource(maxRequests)).filter(_.`type` == "PushEvent").map { x =>
        implicit val formats = DefaultFormats
        PushEvent(x.id, x.repo, x.actor, x.org, x.payload.extract[Payload], x.public, x.created_at)
      }

    //work around for not existing RichAsyncFunction in Scala
    val asyncFunction = new GetOrAddPushEvent
    val finalStream =
      JavaAsyncDataStream.unorderedWait(stream.javaStream, asyncFunction, 10, TimeUnit.SECONDS, 50)

    new org.apache.flink.streaming.api.scala.DataStream(finalStream)
  }

  /**
    * Composes the stream.
    * @param env the environment to compose.
    * @return a future of the method.
    */
  override def compose(env: StreamExecutionEnvironment, queryId: String): Future[Unit] = Async.async {
    val sinkName = s"composedsink_${queryId}"
    val sink = await(SubjectFactory.GetSink[GitHubProtocol.PushEvent](sinkName))
    val stream = getStream(env)
    stream.addSink(sink)
    //stream.addSink(new MongoSink[GitHubProtocol.PushEvent](PUSH_EVENT, "id"))
  }

}
