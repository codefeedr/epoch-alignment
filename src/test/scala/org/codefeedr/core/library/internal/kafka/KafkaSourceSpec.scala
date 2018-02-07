

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

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.codefeedr.core.FullIntegrationSpec
import org.codefeedr.core.library.internal.SubjectTypeFactory
import org.codefeedr.core.library.internal.zookeeper.{ZkClient, ZkNodeBase}
import org.codefeedr.core.library.LibraryServices
import org.codefeedr.Model.TrailedRecord
import org.scalatest.time.Seconds
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, BeforeAndAfterEach}

import scala.async.Async.{async, await}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

case class TestKafkaSourceSubject(prop1: String)

/**
  * Test for [[KafkaTrailedRecordSource]]
  */
class KafkaSourceSpec extends FullIntegrationSpec  {
  val testSubjectName = "TestKafkaSourceSubject"

  "A KafkaSource" should "Register and remove itself in the SubjectLibrary" in async {
    val sourceName = "testSource"
    val subjectType = SubjectTypeFactory.getSubjectType[TestKafkaSourceSubject]
    val subjectNode = subjectLibrary.GetSubject(subjectType.name)
    //Create the node, normally the sinks are responsible for the creation of the subject node
    await(subjectNode.Create(subjectType))
    assert(await(subjectNode.GetState()).get)

    val subject = await(subjectNode.GetOrCreateType[TestKafkaSourceSubject]())

    val source = new KafkaTrailedRecordSource(subject, sourceName)
    val sourceNode = subjectNode.GetSources().GetChild(sourceName)

    assert(!await(sourceNode.Exists()))
    source.InitRun()

    assert(await(sourceNode.Exists()))
    assert(await(sourceNode.GetConsumers().GetState()))
    assert(await(subjectNode.GetSources().GetState()))
    assert(source.running)

    //Since the subject has no sources, just calling update should close it
    await(subjectNode.UpdateState())

    await(subjectNode.AwaitClose())

    //HACK: Somehow need to wait until the callback on the source has fired
    Thread.sleep(100)

    assert(!source.running)
  }
}
