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
import org.codefeedr.core.library.internal.Plugin
import org.codefeedr.plugins.github.{GitHubCommitsPlugin, GitHubEventsPlugin}
import org.codefeedr.plugins.github.clients.GitHubProtocol.{Commit, PushEvent}
import org.codefeedr.plugins.github.clients.MongoDB

import scala.concurrent.Await
import scala.async.Async._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.bson._

import scala.io.StdIn
import scala.util.{Failure, Success}

object Main {

  //all plugins we currenly have
  val plugins: List[Plugin] = new GitHubEventsPlugin :: new GitHubCommitsPlugin :: Nil

  //keep track if application is running
  var running = false

  def main(args: Array[String]): Unit = {
    running = true

    while (running) {
      println("-- Choose an option --")
      println("0) Stop application")

      //print all plugins
      plugins.zipWithIndex.foreach {
        case (plugin, count) => println(s"${count + 1}) Start ${plugin.getClass.getName} job.")
      }

      val input = StdIn.readInt()
      processInput(input)
    }
    println("Now stopping application.")
  }

  /**
    * Processes the console input.
    * @param input the console input.
    */
  def processInput(input: Int) = input match {
    case 0 => running = false
    case x if x <= plugins.size => runPlugin(x)
    case _ => println("Unrecognized command, please retry.")
  }

  /**
    * Runs a plugin.
    * @param id the id of the plugin.
    */
  def runPlugin(id: Int) = {
    val plugin = plugins(id - 1) //list starts from 0

    val output = Await.ready(plugin.run(), Duration.Inf)

    //print output if there is one.
    println(output.value.get)
  }

}
