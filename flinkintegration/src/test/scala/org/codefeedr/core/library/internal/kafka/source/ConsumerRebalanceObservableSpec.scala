package org.codefeedr.core.library.internal.kafka.source

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.TopicPartition
import org.scalatest.{AsyncFlatSpec, FlatSpec}

import scala.collection.JavaConverters._
import org.codefeedr.util.observableExtension.FutureObservable

import scala.async.Async.{async, await}

/**
  * Test class for [[ConsumerRebalanceObservable]]
  */
class ConsumerRebalanceObservableSpec extends AsyncFlatSpec with LazyLogging{


  "ConsmerRebalanceObservable.observePartitions" should "produce an event when new partitions are assigned" in async {
    //Arrange
    val subject = new RebalanceListenerImpl()

    //Act
    val f = subject.observePartitions().take(2).collectAsFuture()
    subject.onPartitionsAssigned(List(new TopicPartition("a",1)).asJava)
    subject.onPartitionsAssigned(List(new TopicPartition("a",2)).asJava)
    val r = await(f)


    //Assert
    val initial = r.head
    val updated = r.tail.head

    assert(initial.size == 1)
    assert(initial.exists(_ == new TopicPartition("a",1)))

    assert(updated.size == 2)
    assert(updated.exists(_ == new TopicPartition("a",1)))
    assert(updated.exists(_ == new TopicPartition("a",2)))
  }
}
