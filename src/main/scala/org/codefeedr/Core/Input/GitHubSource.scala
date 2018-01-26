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

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.codefeedr.Core.Clients.GitHubAPI
import org.eclipse.egit.github.core.event.Event
import org.eclipse.egit.github.core.service.EventService

import scala.collection.JavaConversions._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

class GitHubSource(maxRequests: Integer = -1) extends RichSourceFunction[Event] {

  //get logger used by Flink
  val log: Logger = LoggerFactory.getLogger(classOf[GitHubSource])

  //loads the github api
  lazy val GitHubAPI: GitHubAPI = new GitHubAPI()

  //amount of events polled after closing
  var eventsPolled: Integer = 0

  //keeps track if the event polling is still running
  var isRunning = true

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

    val es = new EventService(GitHubAPI.client)

    //keep track of the request
    var currentRequest = 0

    while (isRunning) {

      //make sure only 1 parallel process pulls
      if (getRuntimeContext.getIndexOfThisSubtask % 1 != 0) {
        isRunning = false
        return
      }

      //get the events per poll
      val it = es.pagePublicEvents(GitHubAPI.eventsPerPoll)

      while (it.hasNext) {
        it.next.foreach { e =>
          eventsPolled += 1
          ctx.collectWithTimestamp(e, e.getCreatedAt.getTime) //output all with timestamp
        }
      }

      currentRequest += 1

      //this part is just for debugging/test purposes
      if (maxRequests != -1 && currentRequest >= maxRequests) {
        cancel()

        log.info(s"Going to send $eventsPolled events")
        return
      }

      synchronized {
        //wait to not exceed the rate limit of Github API
        wait(GitHubAPI.waitingTime * 1000L)
      }
    }
  }
}
