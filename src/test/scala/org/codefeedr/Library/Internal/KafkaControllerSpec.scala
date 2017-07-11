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

  "A controller" should "be able to create new topics" in {
    for {
      _ <- KafkaController.CreateTopic(testTopic)
      validate <- KafkaController.GetTopics().map(o => assert(o.contains(testTopic)))
    } yield validate
  }

  "A controller" should "be able to remove a newly created topic. If this fails in kafka 'delete.topic.enable' is set to false (by default)" in {
    for {
      _ <- KafkaController.DeleteTopic(testTopic)
      list <- KafkaController.GetTopics().map(o => assert(!o.contains(testTopic)))
    } yield list
  }

}
