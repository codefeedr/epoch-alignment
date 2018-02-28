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
package org.codefeedr.plugins.github.jobs

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.{AsyncDataStream => JavaAsyncDataStream}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.codefeedr.core.library.internal.Job
import org.codefeedr.plugins.github.clients.GitHubProtocol.{Commit, Event, Payload, PushEvent}
import org.codefeedr.plugins.github.input.GitHubSource
import org.codefeedr.plugins.github.operators.GetOrAddPushEvent
import org.codefeedr.plugins.github.serialization.JsonPushEventSerialization
import org.json4s.DefaultFormats

import scala.async.Async.async
import scala.concurrent.Future
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.reflect.runtime.{universe => ru}

/**
  * Retrieves events from GitHubSource and pushes to Kafka.
  * @param maxRequests the maximum requests for the event polling.
  */
class EventsJob(maxRequests: Int = -1) extends Job[Event, PushEvent]("events_job") {

  //set parallelism for this job
  override def getParallelism: Int = 2

  //lazily load config
  lazy val config = ConfigFactory.load()

  //define kafka/zk related information
  val topicId = "push_events" //we want to push to this topic for push events
  val kafka = config.getString("codefeedr.kafka.server.bootstrap.servers")
  val zKeeper = config.getString("codefeedr.zookeeper.connectionstring")

  @transient //serialization schema for the events
  val serSchema = new JsonPushEventSerialization()

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
        //parse to PushEvent
        PushEvent(x.id, x.repo, x.actor, x.org, x.payload.extract[Payload], x.public, x.created_at)
      }

    //work around for not existing RichAsyncFunction in Scala
    val getPush = new GetOrAddPushEvent //get or add push event to mongo
    val finalStream = //time out of 10 seconds, with 50 concurrent async calls
      JavaAsyncDataStream.unorderedWait(stream.javaStream, getPush, 10, TimeUnit.SECONDS, 50)

    new DataStream(finalStream)
  }

  /**
    * Composes the execution environment
    * @param env the environment where the source should be composed on.
    * @param queryId the id of this 'query'.
    */
  override def compose(env: StreamExecutionEnvironment, queryId: String): Future[Unit] = async {
    //setup kafka
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", kafka)
    prop.setProperty("group.id", topicId)
    prop.setProperty("zookeeper.connect", zKeeper)
    prop.setProperty("max.request.size", "10000000") //+- 10 mb

    //setup producer and add as sink
    val sink = new FlinkKafkaProducer010[PushEvent](topicId, serSchema, prop)
    val stream = getStream(env)
    stream.addSink(sink)
  }

}
