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
import org.codefeedr.core.clients.github.GitHubProtocol.{Commit, PushEvent, SimpleCommit}
import org.apache.flink.streaming.api.datastream.{AsyncDataStream => JavaAsyncDataStream}
import org.codefeedr.core.library.internal.{Job, Plugin}
import org.codefeedr.core.operators.{CheckAndForwardCommit, GetOrAddCommit, GetOrAddPushEvent}
import org.apache.flink.streaming.api.scala._

class RetrieveCommitsJob extends Job[PushEvent, Commit]("retrieve_commits") {

  override def getParallelism: Int = 50

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
      JavaAsyncDataStream.unorderedWait(stream.javaStream, getCommit, 10, TimeUnit.SECONDS, 10)

    new DataStream(finalStream)
  }
}
