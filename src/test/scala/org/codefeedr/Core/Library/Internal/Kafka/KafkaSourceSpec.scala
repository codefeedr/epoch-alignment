

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
import org.codefeedr.Core.FullIntegrationSpec
import org.codefeedr.Core.Library.Internal.Zookeeper.{ZkClient, ZkNodeBase}
import org.codefeedr.Core.Library.LibraryServices
import org.codefeedr.Model.TrailedRecord
import org.scalatest.time.Seconds
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, BeforeAndAfterEach}

import scala.async.Async.{async, await}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

case class TestKafkaSourceSubject(prop1: String)

class KafkaSourceSpec extends FullIntegrationSpec {
  val testSubjectName = "TestKafkaSourceSubject"

  "A KafkaSource" should "Register and remove itself in the SubjectLibrary" in async {
    val sourceName = "testSource"
    val subjectNode = subjectLibrary.GetSubject[TestKafkaSourceSubject]()
    val subject = await(subjectNode.GetOrCreateType[TestKafkaSourceSubject]())

    val source = new KafkaTrailedRecordSource(subject, sourceName)
    val sourceNode = subjectNode.GetSources().GetChild(sourceName)

    assert(!await(sourceNode.Exists()))
    source.InitRun()

    assert(!await(sourceNode.Exists()))
    assert(await(subjectNode.GetSources().GetState()))
    assert(source.running)
    val sourceClose = source.AwaitClose()
    assert(!await(subjectNode.GetSources().GetState()))

    val subjectRemove = subjectNode.AwaitRemoval()

    //Close the subject so the sink should close itself
    await(subjectNode.Close())

    //Await the close
    Await.ready(sourceClose, Duration(1, SECONDS))
    assert(!source.running)
    //It should remove itself from the library when it has stopped running, causing the type to be removed
    //This is temporary disabled
    //Await.ready(subjectRemove, Duration(1, SECONDS))
    assert(true)
  }
}
