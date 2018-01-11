

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

package org.codefeedr.Core.Library.Internal.Kafka

import org.codefeedr.Core.Library.Internal.Kafka.Sink.KafkaGenericSink
import org.codefeedr.Core.Library.Internal.Zookeeper.ZkClient
import org.codefeedr.Core.Library.{LibraryServices, SubjectLibrary}
import org.codefeedr.Core.LibraryServiceSpec
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, BeforeAndAfterEach}

import scala.async.Async.{async, await}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future, TimeoutException}

case class TestKafkaSinkSubject(prop1: String)

class KafkaSinkSpec extends LibraryServiceSpec with BeforeAndAfterEach with BeforeAndAfterAll {


  val testSubjectName = "TestKafkaSinkSubject"

  override def beforeEach(): Unit = {
    Await.ready(zkClient.DeleteRecursive("/"), Duration(1, SECONDS))
    Await.ready(subjectLibrary.Initialize(),Duration(1, SECONDS))
  }

  "A KafkaSink" should "Register and remove itself in the SubjectLibrary" in async {
    val subject = await(subjectLibrary.GetOrCreateType[TestKafkaSinkSubject]())
    await(subjectLibrary.RegisterSource(testSubjectName, "SomeSource"))
    val sink = new KafkaGenericSink[TestKafkaSinkSubject](subject)
    assert(!await(subjectLibrary.GetSinks(testSubjectName)).contains(sink.uuid.toString))
    sink.open(null)
    assert(await(subjectLibrary.GetSinks(testSubjectName)).contains(sink.uuid.toString))
    sink.close()
    val r = assert(!await(subjectLibrary.GetSinks(testSubjectName)).contains(sink.uuid.toString))
    await(subjectLibrary.UnRegisterSource(testSubjectName, "SomeSource"))
    r
  }
}
