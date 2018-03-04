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
package org.codefeedr.core.library.internal

import org.codefeedr.core.library.LibraryServices
import org.codefeedr.core.library.metastore.MetaRootNode

import scala.concurrent.{Await, Future}
import scala.concurrent._
import ExecutionContext.Implicits.global
import async.Async._
import scala.concurrent.duration.{Duration, SECONDS}

abstract class Plugin {

  //setup library and zk client
  lazy val subjectLibrary = LibraryServices.subjectLibrary
  lazy val zkClient = LibraryServices.zkClient

  //keep track of job list
  var jobs: List[Job[_, _]] = List()

  /**
    * Starts a plugin.
    */
  private def startPlugin() = async {
    await(subjectLibrary.initialize())
    await(new MetaRootNode().deleteRecursive())

    //setups job and set correct type
    jobs = await(setupJobs)
    jobs.foreach(_.setupType(subjectLibrary))
  }

  /**
    * Stops a plugin.
    */
  private def stopPlugin() = async {
    await(new MetaRootNode().deleteRecursive())
  }

  /**
    * Setup jobs.
    * @return a list of jobs.
    */
  def setupJobs: Future[List[Job[_, _]]]

  /**
    * Runs a job.
    */
  def run() = async {
    await(startPlugin())
    //await(Future.sequence(jobs.map(_.startJob(subjectLibrary))))

    //start and finish all jobs in order
    for (job <- jobs) {
      Await.ready(job.startJob(), Duration.Inf)
      Await.ready(subjectLibrary.getSubject(job.subjectType.name).assertExists(), Duration.Inf)
    }

    await(stopPlugin())
  }
}
