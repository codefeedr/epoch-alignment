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
  //TODO: move this to some sort of configuration
  private lazy val properties: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("retries", Predef.int2Integer(0))
    props.put("batch.size", Predef.int2Integer(16384))
    props.put("linger.ms", Predef.int2Integer(1))
    props.put("buffer.memory", Predef.int2Integer(33554432))
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

  /**
    * Perform a method on the kafka admin. Using a managed resource to dispose of the admin client after use
    * TODO: Handle exceptions from the creation of the amdin client
    * @param method the method to run on the kafka cluster
    * @tparam T return type of the method
    * @return raw result from kafka API
    */
  private def apply[T](method: AdminClient => T): T =
    (managed(AdminClient.create(properties)) map method).opt.get

  /**
    * Create a new topic on kafka
    * @param name name of the topic to register
    * @return a future that resolves when the topic has been created
    */
  def CreateTopic(name: String): Future[Unit] = {
    //TODO: Support numTopics and replication factor. Probably need to integrate this with flink?
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
