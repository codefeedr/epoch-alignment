

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

package org.codefeedr.core.library.internal.kafka

import org.codefeedr.core.library.internal.kafka.sink.KafkaGenericSink
import org.codefeedr.core.library.internal.zookeeper.ZkClient
import org.codefeedr.core.library.LibraryServices
import org.codefeedr.core.LibraryServiceSpec
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, BeforeAndAfterEach}

import scala.async.Async.{async, await}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future, TimeoutException}

case class TestKafkaSinkSubject(prop1: String)

/**
  * Test for [[KafkaGenericSink]]
  */
class KafkaSinkSpec extends LibraryServiceSpec with BeforeAndAfterEach with BeforeAndAfterAll {


  val testSubjectName = "TestKafkaSinkSubject"



  "A KafkaSink" should "Register and remove itself in the SubjectLibrary" in async {
    val sinkId = "testSink"
    val subjectNode = subjectLibrary.GetSubject[TestKafkaSinkSubject]()
    val subject = await(subjectNode.GetOrCreateType[TestKafkaSinkSubject]())

    val sink = new KafkaGenericSink[TestKafkaSinkSubject](subject,sinkId)
    val sinkNode = subjectNode.GetSinks().GetChild(sinkId)

    assert(!await(sinkNode.Exists()))
    sink.open(null)
    assert(await(sinkNode.Exists()))
    assert(await(subjectNode.GetSinks().GetState()))
    sink.close()

    assert(!await(subjectNode.GetSinks().GetState()))
  }

  override def afterEach(): Unit = {
    Await.ready(zkClient.DeleteRecursive("/"), Duration(1, SECONDS))
    Await.ready(subjectLibrary.Initialize(),Duration(1, SECONDS))
  }
}
