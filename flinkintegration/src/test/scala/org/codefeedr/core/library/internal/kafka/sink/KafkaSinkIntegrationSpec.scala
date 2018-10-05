

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

package org.codefeedr.core.library.internal.kafka.sink

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import org.apache.flink.api.common.state.{ListState, OperatorStateStore}
import org.apache.flink.runtime.state.FunctionInitializationContext
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.codefeedr.core.LibraryServiceSpec
import org.codefeedr.core.library.internal.SubjectTypeFactory
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.codefeedr.util.NoEventTime._
import collection.JavaConverters._
import scala.async.Async.{async, await}
import scala.concurrent.Await
import scala.concurrent.duration._
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar

import scala.util.Success

case class TestKafkaSinkSubject(prop1: String)

/**
  * Test for [[KafkaGenericSink]]
  */
class KafkaSinkIntegrationSpec extends LibraryServiceSpec with BeforeAndAfterEach with BeforeAndAfterAll with MockitoSugar {


  val testSubjectName = "TestKafkaSinkSubject"



  "A KafkaSink" should "Register and remove itself in the SubjectLibrary" in async {
    val sinkId = "testSink"

    val subject = SubjectTypeFactory.getSubjectType[TestKafkaSinkSubject]
    val subjectNode = await(libraryServices.subjectFactory.create(subject))

    val job = libraryServices.subjectLibrary.getJob("testJob")

    //,subjectFactory.getTransformer[TestKafkaSinkSubject](subject)
    val sink = new KafkaGenericSink[TestKafkaSinkSubject](subjectNode,job,libraryServices.kafkaConfiguration,libraryServices.kafkaProducerFactory,libraryServices.epochStateManager,sinkId)
    val sinkNode = subjectNode.getSinks().getChild(sinkId)
    val runtimeContext = mock[StreamingRuntimeContext]

    val context = mock[FunctionInitializationContext]
    val operatorStore = mock[OperatorStateStore]
    val listState = mock[ListState[KafkaSinkState]]
    when(context.getOperatorStateStore()) thenReturn(operatorStore)
    when(operatorStore.getListState[KafkaSinkState](ArgumentMatchers.any())) thenReturn(listState)
    when(listState.get()) thenReturn Iterable.empty[KafkaSinkState].asJava


    sink.setRuntimeContext(runtimeContext)


    assert(!await(sinkNode.exists()))
    sink.initializeState(context)
    sink.open(null)


    assert(await(sinkNode.exists()))
    assert(await(subjectNode.getSinks().getState()))
    sink.close()

    assert(!await(subjectNode.getSinks().getState()))
  }


  /**
    * Use -Dsun.io.serialization.extendedDebugInfo=true to debug why the sink is not serializable
    */
  it should "be serializable" in async {
      //Arrange
      val sinkId = "testSink"
      val subject = SubjectTypeFactory.getSubjectType[TestKafkaSinkSubject]
      val subjectNode = await(libraryServices.subjectFactory.create(subject))
      val job = libraryServices.subjectLibrary.getJob("testJob")
      val sink = new KafkaGenericSink[TestKafkaSinkSubject](subjectNode,job,libraryServices.kafkaConfiguration,libraryServices.kafkaProducerFactory,libraryServices.epochStateManager,sinkId)

      //Act
      //Not catching the exception is on purpose
      //because you want the full stack trace of non serializable in case of a failure
      val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(stream)
      oos.writeObject(sink)
      oos.close()
      val t =Success(stream.toByteArray)

      //Assert
      assert(t.isSuccess)
  }



  override def beforeEach(): Unit = {
    super.beforeEach()
    Await.ready(libraryServices.zkClient.deleteRecursive("/"), Duration(1, SECONDS))
    Await.ready(libraryServices.subjectLibrary.initialize(),Duration(1, SECONDS))
  }

  override def afterEach(): Unit = {
    super.afterEach()
    Await.ready(libraryServices.zkClient.deleteRecursive("/"), Duration(1, SECONDS))
    Await.ready(libraryServices.subjectLibrary.initialize(),Duration(1, SECONDS))
  }
}
