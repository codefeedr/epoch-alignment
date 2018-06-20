package org.codefeedr.core.library.internal.kafka.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.types.Row
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.codefeedr.model.{Record, RecordSourceTrail, Source, TrailedRecord}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.codefeedr.util.MockitoExtensions
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FlatSpec}

import scala.collection.mutable

class KafkaSourceConsumerSpec extends FlatSpec with BeforeAndAfterEach with MockitoSugar with MockitoExtensions  {
  private var consumer: KafkaConsumer[RecordSourceTrail, Row] = _
  private val topic = "sourceElement"

  override def beforeEach() = {
    consumer = mock[KafkaConsumer[RecordSourceTrail, Row]]
  }

  "KafkaSourceConsumer.Poll" should "return the latest offsets that it recieved by polling the source" in {
    //Arrange
    val component = constructConsumer()
    when(consumer.poll(ArgumentMatchers.any())) thenReturn constructPollResponse(Seq((1,1L),(1,2L),(2,2L)))

    //Act
    val r = component.poll(_ => Unit)

    //Assert
    assert(r.contains(1 -> 2L))
    assert(r.contains(2 -> 2L))
  }

  it should "invoke ctx with all returned elements" in {
    //Arrange
    val component = constructConsumer()
    when(consumer.poll(ArgumentMatchers.any())) thenReturn constructPollResponse(Seq((1,1L),(1,2L),(2,2L)))
    val queue = new mutable.Queue[SourceElement]

    //Act
    val r = component.poll(queue += _)

    //Assert
    assert(queue.size == 3)
  }

  it should "not treat elements past the given offset" in {
    //Arrange
    val component = constructConsumer()
    when(consumer.poll(ArgumentMatchers.any())) thenReturn constructPollResponse(Seq((1,1L),(1,2L),(1,3L),(2,2L),(2,3L)))
    val queue = new mutable.Queue[SourceElement]

    //Act
    val r = component.poll(queue+= _,Map(1->2,2->2))

    //Assert
    assert(r.contains(1 -> 2L))
    assert(r.contains(2 -> 2L))
    assert(queue.size == 3)
  }

  it should "call seek when it recieved elements past the given offset" in {
    //Arrange
    val component = constructConsumer()
    when(consumer.poll(ArgumentMatchers.any())) thenReturn constructPollResponse(Seq((1,1L),(1,2L),(1,3L),(2,2L),(2,3L)))

    //Act
    val r = component.poll(_ => Unit,Map(1->2,2->2))

    //Assert
    verify(consumer,times(1)).seek(Matches((o: TopicPartition) => o.partition() == 1),ArgumentMatchers.eq(2L))
    verify(consumer,times(1)).seek(Matches((o: TopicPartition) => o.partition() == 2),ArgumentMatchers.eq(2L))
  }


  /** Constructs a simple poll response based on the given partitions and offsets */
  private def constructPollResponse(data: Seq[(Int, Long)]): ConsumerRecords[RecordSourceTrail, Row] = {
    val elements = data.groupBy(o => o._1).map(o => {
      val key = new TopicPartition(topic,o._1)
      val data = o._2.map(o => constructConsumerRecord(o._1,o._2)).toList.asJava
      key -> data
    }).asJava
    new ConsumerRecords[RecordSourceTrail, Row](elements)
  }

  /** Construct some consumerRecord, sufficient for this test class */
  private def constructConsumerRecord(partition: Int, offset: Long): ConsumerRecord[RecordSourceTrail, Row] = {
    new ConsumerRecord[RecordSourceTrail, Row](topic,partition,offset,Source(Array(partition.toByte),Array(offset.toByte)),
      new Row(1))
  }

  private def constructConsumer(): KafkaSourceConsumer[SourceElement,Row,RecordSourceTrail] = {
    new KafkaSourceConsumer[SourceElement,Row,RecordSourceTrail]("sourceConsumer",topic,consumer,mapper)
  }

  private def mapper(value:Row, key:RecordSourceTrail):SourceElement = {
    SourceElement(value.hashCode())
  }

  case class SourceElement(nr: Int)
}
