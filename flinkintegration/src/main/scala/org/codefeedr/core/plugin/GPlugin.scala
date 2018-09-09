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

import org.codefeedr.core.library.CodefeedrComponents
import org.codefeedr.core.library.internal.zookeeper.ZkClientComponent
import org.codefeedr.core.library.internal.{JobComponent, Plugin}
import org.codefeedr.core.library.metastore.SubjectLibraryComponent

import scala.concurrent.Future
import scala.concurrent._
import ExecutionContext.Implicits.global
import async.Async._

class GPlugin extends
  CodefeedrComponents
  with EventsJobFactoryComponent
  with RetrieveCommitsJobComponent
  with JobComponent
  with Plugin {

    override def setupJobs: Future[List[Job[_, _]]] = async {
      //setup events job

      val eventsJob = createEventsJob(1)
      await(eventsJob.setupType())

      //setup commit retrieval job
      val retrieveJob = createRetrieveCommitsJob()
      await(retrieveJob.setupType())
      retrieveJob.setSource(eventsJob)

      eventsJob :: retrieveJob :: Nil
    }



}



