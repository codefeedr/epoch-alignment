package org.codefeedr.core.library.internal.kafka.source

import java.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.types.Row
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.codefeedr.model.{RecordSourceTrail, TrailedRecord}
import rx.lang.scala.subjects.ReplaySubject
import rx.lang.scala.{Observable, Subject}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait KafkaSourceMapper[TElement, TValue, TKey] {
  //Transform the kafka data to an element
  def mapKafkaSource(value:TValue,key:TKey):TElement
}


/**
  * Class performing the polling of kafka for a KafkaSource
  * Converting (java)types we use on Kafka back to (scala) types we use in Flink
  */
class KafkaSourceConsumer[TElement, TValue, TKey](name: String,
                                                  topic: String,
                                                  consumer: KafkaConsumer[TKey, TValue])
    extends LazyLogging {
  this:KafkaSourceMapper[TElement,TValue,TKey] =>

  //Timeout when polling kafka
  @transient private lazy val pollTimeout = 1000

  /**
    * Performs a poll on kafka
    * @param cb callback to invoke for every element
    * @return the last offsets for each partition of the consumer that has been collected in the poll
    */
  def poll(cb: TElement => Unit): Map[Int, Long] = poll(cb, Map[Int, Long]())

  def poll(cb: TElement => Unit, seekOffsets: PartialFunction[Int, Long]): Map[Int, Long] = {
    val shouldInclude = (r: ConsumerRecord[TKey, TValue]) =>
      seekOffsets.lift(r.partition()).forall(_ >= r.offset())
    poll(cb, shouldInclude, seekOffsets)
  }

  private def poll(cb: TElement => Unit,
                   shouldInclude: ConsumerRecord[TKey, TValue] => Boolean,
                   seekOffsets: PartialFunction[Int, Long]): Map[Int, Long] = {
    logger.debug(s"$name started polling")
    val data = consumer.poll(pollTimeout).iterator().asScala
    logger.debug(s"$name completed polling")
    val r = mutable.Map[Int, Long]()
    val resetSet = ArrayBuffer[Int]()
    data.foreach(o => {
      if (shouldInclude(o)) {
        cb(mapKafkaSource(o.value(), o.key()))
        val partition = o.partition()
        val offset = o.offset()
        logger.debug(s"Processing $partition -> $offset ")
        if (!r.contains(partition) || r(partition) < offset) {
          r(partition) = offset
        }
      } else {
        //If we should not include it, we need to reset the consumer after this poll
        val partition = o.partition()
        if (!resetSet.contains(partition)) {
          resetSet += partition
        }
      }
    })
    resetConsumer(resetSet, seekOffsets)

    r.toMap
  }

  private def resetConsumer(partitions: Seq[Int], offsets: PartialFunction[Int, Long]): Unit = {
    if (partitions.nonEmpty) {
      logger.debug(s"Perfoming a seek on $name")
      partitions.foreach(partition => {
        val offset = offsets(partition)
        consumer.seek(new TopicPartition(topic, partition), offset)
      })
    }
  }

  /**
    * Commits the passed offsets
    * @param offsets the offsets to commit
    */
  def commit(offsets: Map[Int, Long]): Unit = {
    //TODO: Implement
    //Do we care about committing a consumer? We already keep track of offsets in the flink managed state...
    /*
    consumer.commitAsync(
      offsets.map(o => (o._1, new OffsetAndMetadata(o._2))).asJava,
      new OffsetCommitCallback {
        override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata],
                                exception: Exception): Unit = {
          val parsedOffset = offsets.asScala.map(o => (o._1, o._2.offset()))
          logger.debug(
            s"Offsetts ${getReadablePartitions(parsedOffset.toMap)} successfully committed to kafka")
        }
      }
    )
   */
  }

  /**
    * Obtains the currently comitted offsets
    * Used to initialize a kafkaSource
    * @return
    */
  def getCurrentOffsets: mutable.Map[Int, Long] =
    mutable.Map[Int, Long]() ++= consumer
      .assignment()
      .asScala
      .map(o => o.partition() -> consumer.position(o))

  /**
    * Retrieves kafka endoffsets -1. (Kafka returns last avalaible message + 1)
    * @return
    */
  def getEndOffsets: Map[Int, Long] =
    consumer
      .endOffsets(consumer.assignment())
      .asScala
      .map(o => o._1.partition() -> (o._2.toLong - 1))
      .toMap

  /**
    * Closes the kafka consumer
    */
  def close(): Unit = {
    consumer.close()
  }
}


/**
  * Companion object for Kafka Source consumer, with initialization logic
  */
object KafkaSourceConsumer {
  /**
    * Construct a new KafkaSourceConsumer
    * @param sourceUuid unique identifier of the Flink source that uses this consumer
    * @param consumerFactory factory providing kafka consumers
    * @tparam TElement Type of the element provided by the consumer
    * @tparam TValue Type of the value in Kafka
    * @tparam TKey Type of key in Kafka
    * @return
    */
  def apply[TElement, TValue,TKey](name:String, sourceUuid: String, topic:String)(mapper:KafkaSourceMapper[TElement, TValue, TKey])(consumerFactory:KafkaConsumerFactory)= {
    val kafkaConsumer = consumerFactory.create[TKey, TValue](sourceUuid)
    //TODO: Subscribe
    val rebalanceListener = new RebalanceListenerImpl()
    kafkaConsumer.subscribe(Iterable(topic).asJavaCollection, rebalanceListener)

    val sourceConsumer = new KafkaSourceConsumer[TElement,TValue,TKey](name,topic, kafkaConsumer)
      with KafkaSourceMapper[TElement, TValue, TKey] {
      override def mapKafkaSource(value: TValue, key: TKey):TElement = mapper.mapKafkaSource(value,key)
    }

    wire(rebalanceListener,sourceConsumer)
    sourceConsumer
  }

  /**
    * Wires up the observables to the kafkaConsumer to update it's partitions as the events come in
    * @param observable consumer rebalance observable, passing all rebalance events
    * @param kafkaConsumer the consumer to wire
    * @tparam TElement Type of the element provided by the consumer
    * @tparam TValue Type of the value in Kafka
    * @tparam TKey Type of key in Kafka
    */
  private def wire[TElement, TValue,TKey](observable:ConsumerRebalanceObservable, kafkaConsumer: KafkaSourceConsumer[TElement, TValue,TKey]) {

  }

}


trait ConsumerRebalanceObservable {
  protected lazy val partitionsRevoked:Subject[Iterable[TopicPartition]] = new ReplaySubject[Iterable[TopicPartition]]()
  protected lazy val partitionsAssigned:Subject[Iterable[TopicPartition]] = new ReplaySubject[Iterable[TopicPartition]]()

  def observePartitionsRevoked():Observable[Iterable[TopicPartition]] = partitionsRevoked

  def observePartitionsAssigned():Observable[Iterable[TopicPartition]] = partitionsAssigned
}

/**
  * Rx Wrapper around
  */
class RebalanceListenerImpl extends ConsumerRebalanceListener with ConsumerRebalanceObservable
{
  override protected def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = partitionsRevoked.onNext(partitions.asScala)

  override protected def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = partitionsAssigned.onNext(partitions.asScala)
}
