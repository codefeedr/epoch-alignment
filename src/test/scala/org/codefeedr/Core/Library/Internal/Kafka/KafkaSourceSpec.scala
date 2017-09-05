

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

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.codefeedr.Core.Library.Internal.Zookeeper.ZkClient
import org.codefeedr.Core.Library.SubjectLibrary
import org.codefeedr.Model.TrailedRecord
import org.scalatest.time.Seconds
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, BeforeAndAfterEach}

import scala.async.Async.{async, await}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

case class TestKafkaSourceSubject(prop1: String)

class KafkaSourceSpec extends AsyncFlatSpec with BeforeAndAfterEach with BeforeAndAfterAll{

  val testSubjectName = "TestKafkaSourceSubject"

  override def beforeEach(): Unit = {
    Await.ready(ZkClient().DeleteRecursive("/"), Duration(1, SECONDS))
    Await.ready(SubjectLibrary.Initialize(), Duration(1, SECONDS))
  }




  "A KafkaSource" should "Register and remove itself in the SubjectLibrary" in async {
    val subject = await(SubjectLibrary.GetOrCreateType[TestKafkaSourceSubject](persistent = false))
    val source = new KafkaSource(subject)
    assert(!await(SubjectLibrary.GetSources(testSubjectName)).contains(source.uuid.toString))
    source.InitRun()
    assert(await(SubjectLibrary.GetSources(testSubjectName)).contains(source.uuid.toString))
    assert(source.running)

    val sourceClose = source.AwaitClose()

    //Close the subject so the sink should close itself
    await(SubjectLibrary.Close(testSubjectName))

    //Await the close
    Await.ready(sourceClose, Duration(1, SECONDS))
    assert(!source.running)
    //It should remove itself from the library when it has stopped running, causing the type to be removed
    Await.ready(SubjectLibrary.AwaitClose(testSubjectName), Duration(1, SECONDS))
    assert(true)
  }
}
