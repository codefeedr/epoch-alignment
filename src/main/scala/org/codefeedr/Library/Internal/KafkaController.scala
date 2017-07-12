package org.codefeedr.Library.Internal

import java.util
import java.util.Properties

import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import resource.managed

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.collection.immutable._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * low level object to control the connected kafka
  */
object KafkaController {

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
    * Still need to support numTopics and replication factor. Probably need to integrate this with flink?
    * @param name name of the topic to register
    * @return a future that resolves when the topic has been created
    */
  def CreateTopic(name: String): Future[Unit] = {
    val topic = new NewTopic(name, 1, 1)
    val topicSet = Iterable(topic).asJavaCollection
    val result = apply(o => o.createTopics(topicSet))
    Future {
      result.all().get()
    }
  }

  /**
    * Get the list of topics registered on the kafka cluster
    * @return a future of a set of topic names
    */
  def GetTopics(): Future[Set[String]] = {
    Future {
      apply(o => o.listTopics()).names().get()
    }.map(o => o.toSet)
  }

  /**
    * Deletes a topic of the given name on the kafka cluster
    * @param topic the name of the topic to remove
    * @return future that resolves when the topic no longer exists on the cluster
    */
  def DeleteTopic(topic: String): Future[Unit] = {
    Future {
      apply(o => o.deleteTopics(Iterable(topic).asJavaCollection)).all().get()
    }
  }
}
