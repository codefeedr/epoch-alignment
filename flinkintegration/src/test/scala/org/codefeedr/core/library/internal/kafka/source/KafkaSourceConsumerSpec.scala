package org.codefeedr.core.library.internal.kafka.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.types.Row
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.codefeedr.model.{Record, RecordSourceTrail, Source, TrailedRecord}

import rx.lang.scala.{Observable, Subject}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.codefeedr.util.MockitoExtensions
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FlatSpec}

import scala.collection.mutable

class KafkaSourceConsumerSpec extends FlatSpec with BeforeAndAfterEach with MockitoSugar with MockitoExtensions {
  private var consumer: KafkaConsumer[RecordSourceTrail, Row] = _
  private val topic = "sourceElement"


  override def beforeEach() = {
    consumer = mock[KafkaConsumer[RecordSourceTrail, Row]]
  }

  "KafkaSourceConsumer.Poll" should "update the latest offsets that it recieved by polling the source" in {
    //Arrange
    val component = constructConsumer()
    when(consumer.poll(ArgumentMatchers.any())) thenReturn constructPollResponse(Seq((1, 1L), (1, 2L), (2, 2L)))

    //Act
    component.poll(_ => Unit)

    //Assert
    assert(component.getCurrentOffsets.contains(1 -> 2L))
    assert(component.getCurrentOffsets.contains(2 -> 2L))
  }

  it should "invoke ctx with all returned elements" in {
    //Arrange
    val component = constructConsumer()
    when(consumer.poll(ArgumentMatchers.any())) thenReturn constructPollResponse(Seq((1, 1L), (1, 2L), (2, 2L)))
    val queue = new mutable.Queue[SourceElement]

    //Act
    val r = component.poll(queue += _)

    //Assert
    assert(queue.size == 3)
  }

  it should "not treat elements past the given offset" in {
    //Arrange
    val component = constructConsumer()
    when(consumer.poll(ArgumentMatchers.any())) thenReturn constructPollResponse(Seq((1, 1L), (1, 2L), (1, 3L), (2, 2L), (2, 3L)))
    val queue = new mutable.Queue[SourceElement]

    //Act
    component.poll(queue += _, Map(1 -> 2, 2 -> 2))

    //Assert
    assert(component.getCurrentOffsets.contains(1 -> 2L))
    assert(component.getCurrentOffsets.contains(2 -> 2L))
    assert(queue.size == 3)
  }

  it should "call seek when it recieved elements past the given offset" in {
    //Arrange
    val component = constructConsumer()
    when(consumer.poll(ArgumentMatchers.any())) thenReturn constructPollResponse(Seq((1, 1L), (1, 2L), (1, 3L), (2, 2L), (2, 3L)))

    //Act
    component.poll(_ => Unit, Map(1 -> 2, 2 -> 2))

    //Assert
    verify(consumer, times(1)).seek(Matches((o: TopicPartition) => o.partition() == 1), ArgumentMatchers.eq(2L))
    verify(consumer, times(1)).seek(Matches((o: TopicPartition) => o.partition() == 2), ArgumentMatchers.eq(2L))
  }


  it should "add a default offset when a partition is assigned, but no data has been received" in {
    //Arrange
    val component = constructConsumer()
    when(consumer.poll(ArgumentMatchers.any())) thenReturn constructPollResponse(Seq((1, 1L), (1, 2L), (2, 2L)))
    val assignment = Iterable(new TopicPartition("a", 1), new TopicPartition("a", 2), new TopicPartition("a", 3))

    //Act
    component.onNewAssignment(assignment)
    component.poll(_ => Unit)

    //Assert
    assert(component.getCurrentOffsets.contains(1 -> 2L))
    assert(component.getCurrentOffsets.contains(2 -> 2L))
    assert(component.getCurrentOffsets.contains(3 -> -1L))
  }

  it should "remove an offset when no longer subscribed to a topic" in {
    val component = constructConsumer()
    when(consumer.poll(ArgumentMatchers.any())) thenReturn constructPollResponse(Seq((1, 1L), (1, 2L), (2, 2L)))
    val assignment1 = Iterable(new TopicPartition("a", 1), new TopicPartition("a", 2), new TopicPartition("a", 3))
    val assignment2 = Iterable(new TopicPartition("a", 2), new TopicPartition("a", 3))
    //Act
    component.onNewAssignment(assignment1)
    component.poll(_ => Unit)
    when(consumer.poll(ArgumentMatchers.any())) thenReturn constructPollResponse(Seq((2, 3L)))
    component.onNewAssignment(assignment2)
    component.poll(_ => Unit)

    assert(!component.getCurrentOffsets.contains(1 -> 2L))
    assert(component.getCurrentOffsets.contains(2 -> 3L))
    assert(component.getCurrentOffsets.contains(3 -> -1L))
  }

  /** Constructs a simple poll response based on the given partitions and offsets */
  private def constructPollResponse(data: Seq[(Int, Long)]): ConsumerRecords[RecordSourceTrail, Row] = {
    val elements = data.groupBy(o => o._1).map(o => {
      val key = new TopicPartition(topic, o._1)
      val data = o._2.map(o => constructConsumerRecord(o._1, o._2)).toList.asJava
      key -> data
    }).asJava
    new ConsumerRecords[RecordSourceTrail, Row](elements)
  }

  /** Construct some consumerRecord, sufficient for this test class */
  private def constructConsumerRecord(partition: Int, offset: Long): ConsumerRecord[RecordSourceTrail, Row] = {
    new ConsumerRecord[RecordSourceTrail, Row](topic, partition, offset, Source(Array(partition.toByte), Array(offset.toByte)),
      new Row(1))
  }

  private def constructConsumer(): KafkaSourceConsumer[SourceElement, Row, RecordSourceTrail] = {
    new KafkaSourceConsumer[SourceElement, Row, RecordSourceTrail]("sourceConsumer", "topic", consumer) with TestMapper
  }


  case class SourceElement(nr: Int)

  trait TestMapper extends KafkaSourceMapper[SourceElement, Row, RecordSourceTrail] {
    override def transform(value: Row, key: RecordSourceTrail): SourceElement = {
      SourceElement(value.hashCode())
    }
  }

}


