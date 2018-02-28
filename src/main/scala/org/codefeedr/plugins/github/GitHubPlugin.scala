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

import com.sksamuel.avro4s.AvroSchema
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import org.codefeedr.core.library.internal.{Job, Plugin}
import org.codefeedr.plugins.github.clients.GitHubProtocol.{Commit, PushEvent}
import org.codefeedr.plugins.github.jobs.{EventToCommitsJob, EventsJob, RetrieveCommitsJob}

import scala.concurrent.Future
import scala.concurrent._
import ExecutionContext.Implicits.global
import async.Async._
class GitHubPlugin extends Plugin {

  /**
    * Setup all jobs.
    * @return a list of jobs.
    */
  override def setupJobs: Future[List[Job[_, _]]] = async {
    //setup events job
    val eventsToCommitsJob = new EventToCommitsJob()
    await(eventsToCommitsJob.setupType(subjectLibrary))

    eventsToCommitsJob :: Nil
  }

}
