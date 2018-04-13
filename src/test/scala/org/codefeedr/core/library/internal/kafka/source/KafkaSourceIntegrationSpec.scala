

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

package org.codefeedr.core.library.internal.kafka.source

import org.codefeedr.core.FullIntegrationSpec
import org.codefeedr.core.library.internal.SubjectTypeFactory
import org.codefeedr.core.library.internal.kafka.KafkaTrailedRecordSource

import scala.async.Async.{async, await}
import scala.concurrent.Future

case class TestKafkaSourceSubject(prop1: String)

/**
  * Test for [[KafkaTrailedRecordSource]]
  */
class KafkaSourceIntegrationSpec extends FullIntegrationSpec  {
  val testSubjectName = "TestKafkaSourceSubject"
/*
  "A KafkaSource" should "Register and remove itself in the SubjectLibrary" in async {
    val sourceName = "testSource"
    val subjectType = SubjectTypeFactory.getSubjectType[TestKafkaSourceSubject]
    val subjectNode = subjectLibrary.getSubject(subjectType.name)
    //Create the node, normally the sinks are responsible for the creation of the subject node
    await(subjectNode.create(subjectType))
    assert(await(subjectNode.getState()).get)

    val subject = await(subjectNode.getOrCreateType[TestKafkaSourceSubject]())

    val source = new KafkaTrailedRecordSource(subjectNode, sourceName)
    val sourceNode = subjectNode.getSources().getChild(sourceName)

    assert(!await(sourceNode.exists()))
    source.initRun()

    assert(await(sourceNode.exists()))
    assert(await(sourceNode.getConsumers().getState()))
    assert(await(subjectNode.getSources().getState()))
    assert(source.running)

    //Since the subject has no sources, just calling update should close it
    await(subjectNode.updateState())

    await(subjectNode.awaitClose())
    source.snapshotState()
    source.

    //HACK: Somehow need to wait until the callback on the source has fired
    await(Future {
      while (source.running) {
        Thread.sleep(1)
      }
    })

    assert(!source.running)
  }
  */
}
