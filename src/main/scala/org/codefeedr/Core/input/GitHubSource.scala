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

package org.codefeedr.Core.input

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.eclipse.egit.github.core.client.GitHubClient
import org.eclipse.egit.github.core.event.Event
import org.eclipse.egit.github.core.service.EventService

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

class GitHubSource(maxRequests : Integer = -1) extends RichSourceFunction[Event] {

  //get logger used by Flink
  val log : Logger = LoggerFactory.getLogger(classOf[GitHubSource])

  //Github API rate limit
  val rateLimit : Integer = 5000

  //waiting time between request so there are no conflicts with the rate limit
  val waitingTime = rateLimit / 3600

  //the amount of events requested per poll
  var eventsPerPoll = 100 //maximum of 300 events per request TODO: Check this

  //get the codefeedr configuration files
  lazy val conf: Config = ConfigFactory.load()

  //initialize githubclient
  @transient
  lazy val client: GitHubClient = new GitHubClient

  //keeps track if the event polling is still running
  var isRunning = true

  def SetOAuthToken() = {
    client.setOAuth2Token(conf.getString("codefeedr.input.github.apikey"))
  }

  override def cancel(): Unit = {
    log.info("Closing connection with Github API")
    isRunning = false
  }

  //TODO think about parallel jobs here, modulo id?
  override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
    log.info("Opening connection with Github API")
    SetOAuthToken()
    val es = new EventService(client)

    var currentRequest = 0

    while (isRunning) {

      synchronized {
        //get the events per poll
        val it = es.pagePublicEvents(eventsPerPoll)

        while (it.hasNext) {
          it.next.asScala.foreach(e => ctx.collectWithTimestamp(e, e.getCreatedAt.getTime)) //output all with timestamp
        }

        currentRequest += 1

        if (maxRequests != -1 && currentRequest >= maxRequests) {
          log.info("Closing source after " + maxRequests + " requests.")
          isRunning = false

          return
        }

        //wait to not exceed the rate limit of Github API
        wait(waitingTime * 1000L)
      }
    }
  }

}
