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
import org.apache.flink.streaming.api.datastream.{AsyncDataStream => JavaAsyncDataStream}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.codefeedr.core.library.SubjectFactory
import org.codefeedr.core.library.internal.Job

import scala.concurrent._
import ExecutionContext.Implicits.global
import org.codefeedr.plugins.github.clients.GitHubProtocol.{Commit, PushEvent, SimpleCommit}
import org.codefeedr.plugins.github.operators.GetOrAddCommit
import org.codefeedr.plugins.github.serialization.JsonCommitSerialization

import scala.async.Async.{async, await}
import scala.concurrent.Future

class RetrieveCommitsJob extends Job[PushEvent, Commit]("retrieve_commits") {

  lazy val config = ConfigFactory.load()

  //define kafka/zk related information
  val topicId = "commits"
  val kafka = config.getString("codefeedr.kafka.server.bootstrap.servers")
  val zKeeper = config.getString("codefeedr.zookeeper.connectionstring")

  @transient
  val serSchema = new JsonCommitSerialization()

  override def getParallelism: Int = 10

  /**
    * Setups a stream for the given environment.
    *
    * @param env the environment to setup the stream on.
    * @return the prepared datastream.
    */
  override def getStream(env: StreamExecutionEnvironment): DataStream[Commit] = {
    val stream = env
      .addSource(source)
      .flatMap(event => event.payload.commits.map(x => (event.repo.name, SimpleCommit(x.sha))))

    //work around for not existing RichAsyncFunction in Scala
    val getCommit = new GetOrAddCommit //get or add commit to mongo
    val finalStream =
      JavaAsyncDataStream.unorderedWait(stream.javaStream, getCommit, 10, TimeUnit.SECONDS, 100)

    new DataStream(finalStream)
  }

  override def compose(env: StreamExecutionEnvironment, queryId: String): Future[Unit] = async {
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", kafka)
    prop.setProperty("group.id", topicId)
    prop.setProperty("zookeeper.connect", zKeeper)
    prop.setProperty("max.request.size", "10000000") //+- 10 mb

    val sink = new FlinkKafkaProducer010[Commit](topicId, serSchema, prop)
    val stream = getStream(env)
    stream.addSink(sink)
  }
}
