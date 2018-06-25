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

package org.codefeedr.core.library.metastore

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.core.library.internal.SubjectTypeFactory

import scala.reflect.runtime.{universe => ru}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

/**
  * ThreadSafe, Async
  * This class contains services to obtain data about subjects and kafka topics
  *
  * Created by Niels on 14/07/2017.
  */
class SubjectLibrary extends LazyLogging {
  private lazy val root: MetaRootNode = new MetaRootNode()
  private lazy val subjects: SubjectCollectionNode = root.getSubjects()
  private lazy val jobs: JobNodeCollection = root.getJobs()

  /**
    * Initalisation method
    *
    * @return true when initialisation is done
    */
  def initialize(): Future[Boolean] =
    subjects.create().map(_ => true)

  /**
    * Retrieves the nodes representing all registered subjects
    * @return
    */
  def getSubjects(): SubjectCollectionNode = subjects

  /**
    * Retrieves a node representing a single subject of the given name
    * Does not validate if the subject exists
    * @param subjectName name of the subject
    * @return
    */
  def getSubject(subjectName: String): SubjectNode = subjects.getChild(subjectName)

  /**
    * Get the subjectNode based on generic type
    * Uses reflection to obtain name, and then the node
    * @tparam T
    * @return
    */
  def getSubject[T: ClassTag](): SubjectNode = getSubject(SubjectTypeFactory.getSubjectName[T])

  /**
    * Retrieves the jobNode for a job of the given name
    * @param name name of the job
    * @return a jobNode
    */
  def getJob(name: String): JobNode = jobs.getChild(name)
}
