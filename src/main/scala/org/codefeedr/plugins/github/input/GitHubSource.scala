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

package org.codefeedr.plugins.github.input

import com.typesafe.config.ConfigFactory
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.codefeedr.plugins.github.clients.GitHubProtocol.Event
import org.codefeedr.plugins.github.clients.{GitHubAPI, GitHubRequestService}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Functions as a Flink source which retrieves github events.
  * @param maxRequests the maximum of requests this source should do to the GitHub API.
  */
class GitHubSource(maxRequests: Integer = -1) extends RichSourceFunction[Event] {

  //get logger used by Flink
  val log: Logger = LoggerFactory.getLogger(classOf[GitHubSource])

  //loads the github api
  var gitHubAPI: GitHubAPI = _

  //keeps track if the event polling is still running
  var isRunning = false

  /**
    * Called when runtime context is started.
    * @param parameters of this job.
    */
  override def open(parameters: Configuration): Unit = {
    //numbering starts from 0 so we want to increment
    val taskId = getRuntimeContext.getIndexOfThisSubtask + 1

    //initiate GitHubAPI
    gitHubAPI = new GitHubAPI(taskId)

    //we want a different key for a source, so we override it
    gitHubAPI.client.setOAuth2Token(
      ConfigFactory.load().getString("codefeedr.input.github.input_api_key"))

    isRunning = true
  }

  /**
    * Cancels the GitHub API retrieval.
    */
  override def cancel(): Unit = {
    log.info("Closing connection with GitHub API")
    isRunning = false
  }

  /**
    * Runs the source, retrieving data from GitHub API.
    * @param ctx run context.
    */
  override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
    log.info("Opening connection with GitHub API")

    //get github service
    val service: GitHubRequestService = new GitHubRequestService(gitHubAPI.client)

    //keep track of the request
    var currentRequest = 0

    while (isRunning) {

      //get the events per poll
      val events = service.getEvents()

      //collect all the events
      events.foreach { x =>
        ctx.collect(x)
      }

      //update the amount of requests done, only if maxRequests is used
      currentRequest = if (maxRequests == -1) 0 else currentRequest + 1

      //this part is just for debugging/test purposes
      if (maxRequests != -1 && currentRequest >= maxRequests) {
        cancel()

        log.info(s"Going to stop source after $currentRequest requests are done")
        return
      }

      /**
        * We don't want to do more request than the rate limit per key (5000).
        * On a maximum there are 3 request per event poll (3 pages, 100 events).
        * We can do 5000 / 3 = 1666 event polls per hour.
        * 1666 / 60 = 27 polls per minute.
        *
        * 60 / 27 = 2.222 seconds delay in between polls
        */
      Thread.sleep(2222)
    }
  }
}
