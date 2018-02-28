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
package org.codefeedr

import java.util.Date
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import com.sksamuel.avro4s.AvroSchema
import org.bson.types.ObjectId
import akka.actor.{ActorRef, ActorSystem}
import org.bson.BsonDocument
import org.codefeedr.core.plugin.GitHubCommitsPlugin
import org.codefeedr.plugins.github.GitHubEventsPlugin
import org.codefeedr.plugins.github.clients.GitHubProtocol.{Commit, PushEvent}
import org.codefeedr.plugins.github.clients.MongoDB

import scala.concurrent.Await
import scala.async.Async._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.bson._

import scala.util.{Failure, Success}

object Main {

  private lazy val mongoDB = new MongoDB()

  var previousTime = System.currentTimeMillis()

  def main(args: Array[String]): Unit = {
    val plugin = new GitHubEventsPlugin()

    val actorSystem = ActorSystem()
    val scheduler = actorSystem.scheduler
    val task = new Runnable { def run() { runTimer() } }
    scheduler.schedule(initialDelay = Duration(1, TimeUnit.MINUTES),
                       interval = Duration(1, TimeUnit.MINUTES),
                       runnable = task)

    val output = Await.ready(plugin.run(), Duration.Inf)

    output.value.get match {
      case Success(_) => System.exit(0)
      case Failure(_) => System.exit(1)
    }
  }

  def runTimer() = async {
    var currentTime = System.currentTimeMillis()

    var greaterThan = gte("_id", genObjectId(previousTime))
    var smaller = lt("_id", genObjectId(currentTime))

    var events = await(
      mongoDB.getCollection[PushEvent]("github_events").find(and(greaterThan, smaller)).toFuture())

    val uniqueCommits = events.flatMap(x => x.payload.commits.map(y => y.sha)).distinct.size

    val document = Document("beforeDate" -> new Date(previousTime),
                            "afterDate" -> new Date(currentTime),
                            "uniqueCommits" -> uniqueCommits,
                            "eventSize" -> events.size)

    val inserted = await(
      mongoDB
        .getCollection("events_stats")
        .insertOne(document)
        .toFuture())

    println(s"Inserted stats: $inserted")

    previousTime = currentTime //update date
  }

  def genObjectId(timeInMs: Long): ObjectId = {
    var timeInS = timeInMs / 1000L
    var oidString = java.lang.Long.toHexString(timeInS) + "0000000000000000";

    new ObjectId(oidString)
  }

}
