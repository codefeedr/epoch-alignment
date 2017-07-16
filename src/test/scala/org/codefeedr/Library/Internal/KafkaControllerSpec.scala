package org.codefeedr.Library.Internal

import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.internals.Topic
import org.scalatest.{AsyncFlatSpec, FlatSpec, Matchers, fixture}

import scala.collection.JavaConverters._
import scala.concurrent._

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

  "A kafkaController" should "Not add a topic again if guarantee is called and the topic already exists" in {
    for {
      numTopics <- KafkaController.GetTopics().map(o => o.size)
      _ <- KafkaController.GuaranteeTopic(testTopic)
      topics1 <- KafkaController.GetTopics().map(o => assert(o.size == numTopics))
    } yield topics1
  }

  /*
  "A kafkaController" should "be able to destroy all topics on a kafka cluster" in {
    for {
      _ <- KafkaController.CreateTopic("Blablabla")
      topics1 <- KafkaController.GetTopics().map(o => assert(o.size == 2))
      _ <- KafkaController.Destroy()
      topics1 <- KafkaController.GetTopics().map(o => assert(o.isEmpty))
    } yield topics1

  }
  */
}
