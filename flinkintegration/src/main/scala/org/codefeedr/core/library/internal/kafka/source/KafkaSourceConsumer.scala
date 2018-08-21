package org.codefeedr.core.library.internal.kafka.source



import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable
import scala.reflect.ClassTag

trait KafkaSourceMapper[TElement, TValue, TKey] {
  //Transform the kafka data to an element
  def transform(value:TValue, key:TKey):TElement
}


case class KafkaSourceConsumerState(assignment:Option[Iterable[TopicPartition]], offsets:Option[Map[Int,Long]])


/**
  * Class performing the polling of kafka for a KafkaSource
  * Converting (java)types we use on Kafka back to (scala) types we use in Flink
  */
class KafkaSourceConsumer[TElement, TValue, TKey](name: String,
                                                  topic: String,
                                                  consumer: KafkaConsumer[TKey, TValue])
    extends LazyLogging {
  this:KafkaSourceMapper[TElement,TValue,TKey] =>

  //Timeout when polling kafka. TODO: Move to configuration
  private lazy val pollTimeout = 1000

  /**
    * Variable that keeps track of new the assignment of new topicPartitions
    * Make sure to lock when writing to this value
    */
  @volatile private var newPartitions:Option[Iterable[TopicPartition]] = None
  /**
    * Current state of the source consumer
    * Modified during poll loop
    */
  @volatile private var state : KafkaSourceConsumerState = KafkaSourceConsumerState(None,None)

  /**
    * Performs a poll on kafka
    * Updates the state of the kafka consumer
    * @param cb callback to invoke for every element
    */
  def poll(cb: TElement => Unit): Unit = poll(cb, Map[Int, Long]())

  /**
    * Performs a poll on kafka, up to the given offset
    * Updates the state of the kafka consumer
    * @param cb callback to notify new elements to
    * @param seekOffsets maximum offsets to retrieve
    */
  def poll(cb: TElement => Unit, seekOffsets: PartialFunction[Int, Long]):Unit = {
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
        cb(transform(o.value(), o.key()))
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

  /**
    * Updates the assignment of topicPartitions
    * In the case of new partitions, assigns -1 as starting offset
    */
  private def updateAssignments(): Unit =  {
    ???
  }

  private def onNewAssignment(partitions:Iterable[TopicPartition]): Unit = synchronized {
    logger.info(s"New assignment for $name: $partitions")
    newPartitions = Some(partitions)
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
    //Do we care about committing a consumer? We already keep track of offsets in the flink managed state...
    //TODO: Implement or discard
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
    * Returns if the current offsets has passed the endOffsets
    * @param reference offsets to compare to
    * @return None if no assignment has ever been made to this consumer
    *         True if the consumer is further than the reference for all assigned partitions
    *         False if the consumer is behind on one or more assigned partitions
    */
  def higherOrEqual(reference: PartialFunction[Int, Long]): Option[Boolean] =
    state.offsets.map(assignment => assignment.forall(o => reference(o._1) <= o._2))


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
  def apply[TElement, TValue:ClassTag,TKey:ClassTag](name:String, sourceUuid: String, topic:String)(mapper:KafkaSourceMapper[TElement, TValue, TKey])(consumerFactory:KafkaConsumerFactory)= {
    val kafkaConsumer = consumerFactory.create[TKey, TValue](sourceUuid)
    //TODO: Subscribe
    val rebalanceListener = new RebalanceListenerImpl()
    kafkaConsumer.subscribe(Iterable(topic).asJavaCollection, rebalanceListener)

    val sourceConsumer = new KafkaSourceConsumer[TElement,TValue,TKey](name,topic, kafkaConsumer)
      with KafkaSourceMapper[TElement, TValue, TKey] {
      override def transform(value: TValue, key: TKey):TElement = mapper.transform(value,key)
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
    observable.observePartitions().subscribe(kafkaConsumer.onNewAssignment(_))
  }

}


