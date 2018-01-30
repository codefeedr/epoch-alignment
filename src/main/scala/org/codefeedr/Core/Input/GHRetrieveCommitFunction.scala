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

package org.codefeedr.Core.Input

import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import org.codefeedr.Core.Clients.GitHub.{GitHubAPI, GitHubRequestService}
import org.codefeedr.Core.Clients.GitHub.GitHubProtocol._
import scala.async.Async.async
import scala.concurrent.ExecutionContext

/**
  * Retrieves commits asynchronously.
  */
class GHRetrieveCommitFunction extends AsyncFunction[(String, String), Commit] {

  //loads the github api
  lazy val gitHubAPI: GitHubAPI = new GitHubAPI()

  //loads commit service
  lazy val commitService = new GitHubRequestService(gitHubAPI.client)

  /** The context used for the future callbacks */
  implicit lazy val executor: ExecutionContext =
    ExecutionContext.fromExecutor(Executors.directExecutor())

  /**
    * This method dispatches the commit retrieval request.
    * @param input (String, String) tuple, first element is repoName, second element is sha.
    * @param future the future to return the result to.
    */
  override def asyncInvoke(input: (String, String), future: ResultFuture[Commit]): Unit = async {
    val repoName = input._1
    val sha = input._2

    //complete future with the commit
    future.complete(Iterable())
  }

}
