

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

import java.util.UUID

import org.codefeedr.core.library.internal.zookeeper.ZkClient
import org.codefeedr.core.LibraryServiceSpec
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterEach, Matchers}

import scala.async.Async.{async, await}
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}


/**
  * Created by Niels on 11/07/2017.
  */
class KafkaControllerSpec extends LibraryServiceSpec with Matchers with BeforeAndAfterEach {

  private def createTestTopic(): String = s"codefeedr_TestTopic-${UUID.randomUUID()}"

  override def beforeEach(): Unit = {
    super.beforeEach()
    Await.ready(libraryServices.zkClient.deleteRecursive("/"), Duration(5, SECONDS))
    Await.ready(libraryServices.subjectLibrary.initialize(),Duration(5, SECONDS))
  }

  override def afterEach(): Unit = {
    super.afterEach()
    Await.ready(libraryServices.zkClient.deleteRecursive("/"), Duration(5, SECONDS))

  }

  "A kafkaController" should "be able to create and delete new topics" in async {
      val testTopic = createTestTopic()
      await(libraryServices.kafkaController.createTopic(testTopic))
      assert(await(libraryServices.kafkaController.getTopics()).contains(testTopic))
      await(libraryServices.kafkaController.deleteTopic(testTopic))
      assert(!await(libraryServices.kafkaController.getTopics()).contains(testTopic))
  }

  it should "create a new topic if guarantee is called and it does not exist yet" in async {
    val testTopic = createTestTopic()
    await(libraryServices.kafkaController.guaranteeTopic(testTopic))
    assert(await(libraryServices.kafkaController.getTopics()).contains(testTopic))
    await(libraryServices.kafkaController.deleteTopic(testTopic))
    assert(!await(libraryServices.kafkaController.getTopics()).contains(testTopic))
  }

  it should "create a topic with the configured amount of partitions" in async {
      val testTopic = createTestTopic()
      await(libraryServices.kafkaController.guaranteeTopic(testTopic, Some(4)))
      assert(await(libraryServices.kafkaController.getTopics()).contains(testTopic))
      val r = assert(await(libraryServices.kafkaController.getPartitions(testTopic)) == 4)
      await(libraryServices.kafkaController.deleteTopic(testTopic))
      r
  }
}
