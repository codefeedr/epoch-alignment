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

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import resource.managed

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.immutable._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * low level object to control the connected kafka
  */
object KafkaController extends LazyLogging {

  /**
    * Perform a method on the kafka admin. Using a managed resource to dispose of the admin client after use
    * @param method the method to run on the kafka cluster
    * @tparam T return type of the method
    * @return raw result from kafka API
    */
  private def apply[T](method: AdminClient => T): T =
    (managed(AdminClient.create(KafkaConfig.properties)) map method).opt match {
      case None =>
        throw new Exception(
          "Error while connecting to Kafka. Is kafka running and the configuration correct?")
      case Some(value) => value
    }

  /**
    * Create a new topic on kafka
    * For internal use only, does create corresponding state in Zookeeper
    * Number of partitions is passed as parameter
    * @param name name of the topic to register
    * @return a future that resolves when the topic has been created
    */
  def createTopic(name: String, partitions: Int): Future[Unit] = {
    logger.debug(s"Creating kafka topic $name with $partitions partitions")
    val topic = new NewTopic(name, partitions, 1)
    val topicSet = Iterable(topic).asJavaCollection
    val result = apply(o => o.createTopics(topicSet))
    Future {
      result.all().get()
    }
  }

  /**
    * Creates a topic if it does not exist yet. Otherwise does nothing
    * @param name Topic to guarantee
    * @return
    */
  def guaranteeTopic(name: String, partitions: Int): Future[Unit] = {
    getTopics().map(o =>
      if (!o.contains(name)) {
        createTopic(name, partitions)
    })
  }

  /**
    * Gets the number of partitions that the passed topic has
    * @return a future of the number of partitions
    */
  def getPartitions(name: String): Future[Int] = {
    Future {
      val topic = apply(o => o.describeTopics(List(name))).all().get().head
      topic._2.partitions().size()
    }
  }

  /**
    * Get the list of topics registered on the kafka cluster
    * @return a future of a set of topic names
    */
  def getTopics(): Future[Set[String]] = {
    Future {
      apply(o => o.listTopics()).names().get()
    }.map(o => o.toSet)
  }

  /**
    * Deletes a topic of the given name on the kafka cluster
    * @param topic the name of the topic to remove
    * @return future that resolves when the topic no longer exists on the cluster
    */
  def deleteTopic(topic: String): Future[Unit] = {
    Future {
      apply(o => o.deleteTopics(Iterable(topic).asJavaCollection)).all().get()
    }
  }

  /**
    * Destroy all topics on the kafka cluster.
    * @return a set of unit for the destroyed topics
    */
  def destroy(): Future[Set[Unit]] = getTopics().flatMap(o => Future.sequence(o.map(deleteTopic)))
}
