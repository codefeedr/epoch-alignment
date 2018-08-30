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
import org.mockito.{ArgumentMatcher, ArgumentMatchers}
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

  "KafkaSourceConsumer.Poll" should "update the latest offsets that it received by polling the source" in {
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

  it should "call seek when it received elements past the given offset" in {
    //Arrange
    val component = constructConsumer()
    when(consumer.poll(ArgumentMatchers.any())) thenReturn constructPollResponse(Seq((1, 1L), (1, 2L), (1, 3L), (2, 2L), (2, 3L)))

    //Act
    component.poll(_ => Unit, Map(1 -> 2, 2 -> 2))

    //Assert
    verify(consumer, times(1)).seek(Matches((o: TopicPartition) => o.partition() == 1), ArgumentMatchers.eq(2L))
    verify(consumer, times(1)).seek(Matches((o: TopicPartition) => o.partition() == 2), ArgumentMatchers.eq(2L))
  }

  it should "Set the state to the resetted offsets  when it received elements past the given offset" in {
    //Arrange
    val component = constructConsumer()
    when(consumer.poll(ArgumentMatchers.any())) thenReturn constructPollResponse(Seq((1, 1L), (1, 2L), (1, 3L), (2, 2L), (2, 3L)))

    //Act
    component.poll(_ => Unit, Map(1 -> 2, 2 -> 2))

    //Assert
    assert(component.getCurrentOffsets(1) == 2)
    assert(component.getCurrentOffsets(2) == 2)
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

  it should "not overwrite offsets when it already received data for that partition but the assignment arrives later" in {
    //Arrange
    val component = constructConsumer()
    when(consumer.poll(ArgumentMatchers.any())) thenReturn constructPollResponse(Seq((1, 1L), (1, 2L), (2, 2L)))
    val assignment = Iterable(new TopicPartition("a", 1), new TopicPartition("a", 2), new TopicPartition("a", 3))

    //Act
    component.poll(_ => Unit)
    component.onNewAssignment(assignment)
    when(consumer.poll(ArgumentMatchers.any())) thenReturn constructPollResponse(Seq[(Int,Long)]())
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



  "higherOrEqual" should "return None if no partitions have been assigned" in {
    //Arrange
    val component = constructConsumer()
    val partitionOffsetMap = Map(1 -> 2L)
    //Act
    val r = component.higherOrEqual(partitionOffsetMap)
    //Assert
    assert(r.isEmpty)
  }

  it should "return true if the consumer is further than the passed reference for all offsets" in {
    //Arrange
    val sConsumer = constructWithState(Map(1 -> 2,2 -> 2))
    //Act
    val r = sConsumer.higherOrEqual(Map(1 -> 2, 2->2))
    //Assert
    assert(r.get)
  }
  it should "return true if the reference contains fewer partitions than the assignment, but still ahead for all that match" in {
    //Arrange
    val sConsumer = constructWithState(Map(1 -> 2,2 -> 2))
    //Act
    val r = sConsumer.higherOrEqual(Map(1 -> 2))
    //Assert
    assert(r.get)
  }

  it should "return true if the reference contains more partitions than the assignment, but still ahead for all that match" in {
    //Arrange
    val sConsumer = constructWithState(Map(1 -> 2,2 -> 2))
    //Act
    val r = sConsumer.higherOrEqual(Map(1 -> 2,2->2,3->2))
    //Assert
    assert(r.get)
  }

  it should "return false if the consumer is behind on one of the references" in {
    //Arrange
    val sConsumer = constructWithState(Map(1 -> 2,2 -> 2))
    //Act
    val r = sConsumer.higherOrEqual(Map(1 -> 2,2->3))
    //Assert
    assert(!r.get)
  }



  "setPosition" should "call seek ok kafka partition that is part of the assignment" in {
    //Arrange
    val sConsumer = constructWithAssignment(Seq(1,2))

    //Act
    sConsumer.setPosition(Map(1->2,2->3,3->4,0->1))

    //Assert
    //2 calls in total
    verify(consumer, times(2)).seek(ArgumentMatchers.any(),ArgumentMatchers.any())
    verify(consumer, times(1)).seek(ArgumentMatchers.eq(new TopicPartition("a",1)), ArgumentMatchers.eq(2L))
    verify(consumer, times(1)).seek(ArgumentMatchers.eq(new TopicPartition("a",2)), ArgumentMatchers.eq(3L))
  }

  it should "update the currentoffsets" in {
    //Arrange
    val sConsumer = constructWithAssignment(Seq(1,2))

    //Act
    sConsumer.setPosition(Map(1->2,2->3,3->4,0->1))

    //Assert
    assert(sConsumer.getCurrentOffsets.size == 2)
    assert(sConsumer.getCurrentOffsets(1) == 2)
    assert(sConsumer.getCurrentOffsets(2) == 3)
  }

  /** Constructs a consumer with the given offset map as assignment and state **/
  private def constructWithState(offsets: Map[Int,Long]):  KafkaSourceConsumer[SourceElement, Row, RecordSourceTrail] = {
    val sConsumer = constructWithAssignment(offsets.keys)
    //reset the consumer to the passed position
    sConsumer.setPosition(offsets)
    sConsumer
  }

  /** Constructs a consumer with the passed assignment */
  private def constructWithAssignment(partitions: Iterable[Int]): KafkaSourceConsumer[SourceElement, Row, RecordSourceTrail] = {
    val sConsumer = constructConsumer()
    val tps = partitions.map(new TopicPartition("a",_))

    when(consumer.poll(ArgumentMatchers.any())) thenReturn constructPollResponse(Seq[(Int,Long)]())
    sConsumer.onNewAssignment(tps)
    sConsumer.poll(_ => ())
    sConsumer
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


