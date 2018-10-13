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
import org.codefeedr.core.library.internal.SubjectTypeFactory
import org.codefeedr.core.library.internal.kafka.KafkaTrailedRecordSource
import org.codefeedr.model.{SubjectType, TrailedRecord}
import org.codefeedr.util.EventTime
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

class FullIntegrationSpec extends LibraryServiceSpec with Matchers with LazyLogging with BeforeAndAfterEach with MockitoSugar {
  val parallelism: Int = 2

  override def beforeEach(): Unit = {
    super.beforeEach()
    Await.ready(zkClient.deleteRecursive("/"), Duration(5, SECONDS))
    Await.ready(libraryServices.subjectLibrary.initialize(), Duration(5, SECONDS))
  }

  override def afterEach(): Unit = {
    super.afterEach()
    Await.ready(zkClient.deleteRecursive("/"), Duration(5, SECONDS))
  }


  /**
    * Awaits all data of the given subject
    * @param subject the subject to await all data for
    * @return
    */
  def awaitAllData(subject:SubjectType): Future[Array[TrailedRecord]] = async {
    await(libraryServices.subjectLibrary.getSubject(subject.name).assertExists())
    val jobName = UUID.randomUUID().toString
    val job = libraryServices.subjectLibrary.getJob(jobName)
    val source = new KafkaTrailedRecordSource(libraryServices.subjectLibrary.getSubject(subject.name),job,libraryServices.kafkaConfiguration, libraryServices.kafkaConsumerFactory,s"testsource_$jobName")
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
    queryEnv.enableCheckpointing(100)
    logger.debug("Creating query Composer")
    val composer = await(libraryServices.streamComposerFactory.getComposer(query,name))
    logger.debug("Composing queryEnv")
    val resultStream = composer.compose(queryEnv)
    val resultType = composer.getExposedType()
    val node = libraryServices.subjectLibrary.getSubject(resultType.name)
    this.synchronized {
      if (!await(node.exists())) {
        await(libraryServices.subjectFactory.create(resultType))
      }
    }

    logger.debug(s"Composing sink for ${resultType.name}.")
    val sink = libraryServices.subjectFactory.getTrailedSink(resultType,"testSink",name)
    //val sink = await(subjectFactory.get(resultType, name,"testsink"))
    resultStream.addSink(sink)

    logger.debug("Starting queryEnv")
    queryEnv.execute()
    logger.debug("queryenv completed")
    resultType
  }

  /**
    * Execute an environment
    * Currently not very complex, but more logic might be added in the future
    * @param env execution environment
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
  def runSourceEnvironment[T: ru.TypeTag: ClassTag: TypeInformation: EventTime](data: Array[T],useTrailedSink:Boolean = false): Future[SubjectType] = async {
    val t = SubjectTypeFactory.getSubjectType[T]

    if (!await(libraryServices.subjectLibrary.getSubject(t.name).exists())) {
      await(libraryServices.subjectFactory.create(t))
    }



    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism)
    env.enableCheckpointing(1000,CheckpointingMode.EXACTLY_ONCE)
    logger.debug(s"Composing env for ${t.name}")

    await(libraryServices.createCollectionPlugin(data,useTrailedSink).compose(env, "testplugin"))
    logger.debug(s"Starting env for ${t.name}")
    env.execute()
    logger.debug(s"Completed env for ${t.name}")
    t
  }
}