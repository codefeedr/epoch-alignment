/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.codefeedr.Library.Internal.Kafka

import org.scalatest.{AsyncFlatSpec, Matchers}

/**
  * Created by Niels on 11/07/2017.
  */
class KafkaControllerSpec extends AsyncFlatSpec with Matchers {
  val testTopic = "TestTopic"

  "A kafkaController" should "be able to create new topics" in {
    for {
      _ <- KafkaController.CreateTopic(testTopic)
      validate <- KafkaController.GetTopics().map(o => assert(o.contains(testTopic)))
    } yield validate
  }

  "A kafkaController" should "be able to remove a newly created topic. If this fails in kafka 'delete.topic.enable' is set to false (by default)" in {
    for {
      _ <- KafkaController.DeleteTopic(testTopic)
      list <- KafkaController.GetTopics().map(o => assert(!o.contains(testTopic)))
    } yield list
  }

  "A kafkaController" should "create a new topic if guarantee is called and it does not exist yet" in {
    for {
      _ <- KafkaController.GuaranteeTopic(testTopic)
      contains <- KafkaController.GetTopics().map(o => assert(o.contains(testTopic)))
    } yield contains
  }
}
