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

package org.codefeedr.Core

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.StreamTableEnvironment
import org.codefeedr.Core.Engine.Query.{QueryTree, StreamComposerFactory}
import org.codefeedr.Core.Library.Internal.Kafka.KafkaTrailedRecordSource
import org.codefeedr.Core.Library.{LibraryServices, SubjectFactory}
import org.codefeedr.Core.Plugin.CollectionPlugin
import org.codefeedr.Model.{SubjectType, TrailedRecord}
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterEach, FutureOutcome, Matchers}

import scala.async.Async.{async, await}
import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.{Duration, SECONDS}
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

class FullIntegrationSpec extends LibraryServiceSpec with Matchers with LazyLogging with LibraryServices with BeforeAndAfterEach {
  val parallelism: Int = 2

  override def beforeEach(): Unit = {
    Await.ready(subjectLibrary.Initialize(), Duration(1, SECONDS))
  }

  override def afterEach(): Unit = {
    Await.ready(zkClient.DeleteRecursive("/"), Duration(1, SECONDS))
  }


  /**
    * Awaits all data of the given subject
    * @param subject
    * @return
    */
  def AwaitAllData(subject:SubjectType): Future[Array[TrailedRecord]] = async {
    await(subjectLibrary.GetSubject(subject.name).AssertExists())
    val source = new KafkaTrailedRecordSource(subject, "testsource")
    val result = new mutable.ArrayBuffer[TrailedRecord]()
    source.runLocal(result.append(_))
    result.toArray
  }


  /**
    * Utility function that creates a query environment and executes it
    * @param query The query environment
    * @return When the environment is done, the subjectType that was the result of the query
    */
  def RunQueryEnvironment(query: QueryTree): Future[SubjectType] = async {
    val queryEnv = StreamExecutionEnvironment.createLocalEnvironment(parallelism)
    logger.debug("Creating query Composer")
    val composer = await(StreamComposerFactory.GetComposer(query))
    logger.debug("Composing queryEnv")
    val resultStream = composer.Compose(queryEnv)
    val resultType = composer.GetExposedType()
    logger.debug(s"Composing sink for ${resultType.name}.")
    val sink = SubjectFactory.GetSink(resultType, "testsink")
    resultStream.addSink(sink)
    logger.debug("Starting queryEnv")
    queryEnv.execute()
    logger.debug("queryenv completed")
    resultType
  }

  /**
    * Execute an environment
    * Currently not very complex, but more logic might be added in the future
    * @param env
    */
  def runEnvironment(env: StreamExecutionEnvironment): Unit = {
    logger.debug("Starting environment")
    env.execute()
    logger.debug("environment executed")
  }

  /**
    * Utility function for tests that creates a source environment with the given data
    * @param data the data to create environment for
    * @tparam T type of the data
    * @return A future that returns when all data has been pushed to kakfa, with the subjectType that was used
    */
  def RunSourceEnvironment[T: ru.TypeTag: ClassTag: TypeInformation](data: Array[T]): Future[SubjectType] = async {
    val t = await(subjectLibrary.GetSubject[T]().GetOrCreateType[T]())

    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism)
    logger.debug(s"Composing env for ${t.name}")
    await(new CollectionPlugin(data).Compose(env, "testplugin"))
    logger.debug(s"Starting env for ${t.name}")
    env.execute()
    logger.debug(s"Completed env for ${t.name}")
    t
  }
}