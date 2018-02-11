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

package org.codefeedr.core.input

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.codefeedr.core.clients.github.{GitHubAPI, GitHubRequestService}
import org.codefeedr.core.clients.github.GitHubProtocol.Event
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

class GitHubSource(maxRequests: Integer = -1) extends RichSourceFunction[Event] {

  //get logger used by Flink
  val log: Logger = LoggerFactory.getLogger(classOf[GitHubSource])

  //loads the github api
  var gitHubAPI: GitHubAPI = _

  //amount of events polled after closing
  var eventsPolled: Integer = 0

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
        eventsPolled += 1
        ctx.collect(x)
      }

      //update the amount of requests done
      currentRequest += 1

      //this part is just for debugging/test purposes
      if (maxRequests != -1 && currentRequest >= maxRequests) {
        cancel()

        log.info(s"Going to send $eventsPolled events")
        return
      }

      Thread.sleep(2500) //wait 2.5 second
    }
  }
}
