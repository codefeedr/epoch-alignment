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

import scala.concurrent.{Await, Future}
import scala.concurrent._
import ExecutionContext.Implicits.global
import async.Async._
import scala.concurrent.duration.{Duration, SECONDS}

abstract class Plugin {

  lazy val subjectLibrary = LibraryServices.subjectLibrary
  lazy val zkClient = LibraryServices.zkClient

  var jobs: List[Job[_, _]] = List()

  private def startPlugin() = async {
    Await.ready(subjectLibrary.initialize(), Duration(1, SECONDS))
    Await.ready(zkClient.deleteRecursive("/"), Duration(1, SECONDS))
    jobs = await(setupJobs)
  }

  private def stopPlugin() = async {
    Await.ready(zkClient.deleteRecursive("/"), Duration(1, SECONDS))
  }

  def setupJobs: Future[List[Job[_, _]]]

  def run() = async {
    await(startPlugin())
    await(Future.sequence(jobs.map(_.startJob(subjectLibrary))))
    await(stopPlugin())
  }
}
