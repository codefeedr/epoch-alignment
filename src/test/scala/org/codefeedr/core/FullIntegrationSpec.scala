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

package org.codefeedr.core

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.state.{ListState, OperatorStateStore}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.state.FunctionInitializationContext
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.codefeedr.core.engine.query.QueryTree
import org.codefeedr.core.library.internal.kafka.KafkaTrailedRecordSource
import org.codefeedr.core.plugin.CollectionPlugin
import org.codefeedr.model.{SubjectType, TrailedRecord}
import org.scalatest.{BeforeAndAfterEach, Matchers}

import scala.collection.JavaConverters._

//Mockito
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar

import scala.async.Async.{async, await}
import scala.collection.mutable
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

class FullIntegrationSpec extends LibraryServiceSpec with Matchers with LazyLogging with IntegrationTestLibraryServices with BeforeAndAfterEach with MockitoSugar {
  val parallelism: Int = 2

  override def beforeEach(): Unit = {
    Await.ready(subjectLibrary.initialize(), Duration(5, SECONDS))
    Await.ready(zkClient.deleteRecursive("/"), Duration(5, SECONDS))
  }

  override def afterEach(): Unit = {
    Await.ready(zkClient.deleteRecursive("/"), Duration(1, SECONDS))
  }


  /**
    * Awaits all data of the given subject
    * @param subject the subject to await all data for
    * @return
    */
  def awaitAllData(subject:SubjectType): Future[Array[TrailedRecord]] = async {
    await(subjectLibrary.getSubject(subject.name).assertExists())
    val jobName = UUID.randomUUID().toString
    val job = subjectLibrary.getJob(jobName)
    val source = new KafkaTrailedRecordSource(subjectLibrary.getSubject(subject.name),job, kafkaConsumerFactory,"testsource")
    val result = new mutable.ArrayBuffer[TrailedRecord]()


    val initContext = mock[FunctionInitializationContext]
    val operatorStore = mock[OperatorStateStore]
    val listState = mock[ListState[(Int,Long)]]
    when(initContext.getOperatorStateStore) thenReturn operatorStore
    when(operatorStore.getListState[(Int, Long)](ArgumentMatchers.any())) thenReturn listState
    when(listState.get()) thenReturn List[(Int, Long)]().asJava

    val runtimeContext = mock[StreamingRuntimeContext]
    when(runtimeContext.isCheckpointingEnabled) thenReturn false

    //Initialize the source with a mocked sourcecontext
    source.setRuntimeContext(runtimeContext)
    source.initializeState(initContext)
    source.run(new SourceContext[TrailedRecord] {
      override def collectWithTimestamp(element: TrailedRecord, timestamp: Long): Unit = ???

      override def getCheckpointLock: AnyRef = this

      override def markAsTemporarilyIdle(): Unit = ???

      override def emitWatermark(mark: Watermark): Unit = ???

      override def collect(element: TrailedRecord): Unit = result.append(element)

      override def close(): Unit = ???
    })
    result.toArray
  }


  /**
    * Utility function that creates a query environment and executes it
    * Generates a random name to run the query
    * @param query The query environment
    * @return When the environment is done, the subjectType that was the result of the query
    */
  def runQueryEnvironment(query: QueryTree): Future[SubjectType] = async {
    val queryEnv = StreamExecutionEnvironment.createLocalEnvironment(parallelism)
    val name = UUID.randomUUID().toString
    queryEnv.enableCheckpointing(1000)
    logger.debug("Creating query Composer")
    val composer = await(streamComposerFactory.getComposer(query,name))
    logger.debug("Composing queryEnv")
    val resultStream = composer.compose(queryEnv)
    val resultType = composer.getExposedType()
    logger.debug(s"Composing sink for ${resultType.name}.")
    val sink = await(subjectFactory.getSink(resultType, name,"testsink"))
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
  def runSourceEnvironment[T: ru.TypeTag: ClassTag: TypeInformation](data: Array[T]): Future[SubjectType] = async {
    val t = await(subjectLibrary.getSubject[T]().getOrCreateType[T]())

    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism)
    env.enableCheckpointing(100,CheckpointingMode.EXACTLY_ONCE)
    logger.debug(s"Composing env for ${t.name}")
    await(new CollectionPlugin(data).compose(env, "testplugin"))
    logger.debug(s"Starting env for ${t.name}")
    env.execute()
    logger.debug(s"Completed env for ${t.name}")
    t
  }
}