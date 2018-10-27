package org.codefeedr.core.library.internal.kafka.source

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import java.time.Duration
import java.time.temporal.ChronoUnit

import org.codefeedr.core.library.internal.kafka.OffsetUtils

import scala.reflect.ClassTag

trait KafkaSourceMapper[TElement, TValue, TKey] {
  //Transform the kafka data to an element
  def transform(value: TValue, key: TKey): TElement
}

/**
  *   State held by the kafkaSourceConsumer
  * @param assignment
  * @param offsets
  */
case class KafkaSourceConsumerState(assignment: Option[Iterable[TopicPartition]],
                                    offsets: Option[Map[TopicPartition, Long]]) {

  /**
    * Retrieves the current assignment, or empty collection if no assignment exists yet
    * @return
    */
  def getAssignment: Iterable[TopicPartition] =
    assignment.getOrElse(Iterable.empty[TopicPartition])

  /**
    * Retrieves the current offsets, or an empty map if no offsets are available yet
    * @return
    */
  def getOffsets = offsets.getOrElse(Map.empty[TopicPartition, Long])
}

/**
  * Class performing the polling of kafka for a KafkaSource
  * Converting (java)types we use on Kafka back to (scala) types we use in Flink
  */
class KafkaSourceConsumer[TElement, TValue, TKey](name: String,
                                                  topic: String,
                                                  consumer: KafkaConsumer[TKey, TValue])
    extends LazyLogging { this: KafkaSourceMapper[TElement, TValue, TKey] =>

  //Timeout when polling kafka. TODO: Move to configuration
  private lazy val pollTimeout = Duration.of(1, ChronoUnit.SECONDS)

  private def getLabel = name

  @volatile private var emptyAssignments: Int = 0

  /**
    * Variable that keeps track of new the assignment of new topicPartitions
    * Make sure to lock when writing to this value
    */
  @volatile private var newPartitions: Option[Iterable[TopicPartition]] = None

  /**
    * Current state of the source consumer
    * Modified during poll loop
    */
  @volatile private var state: KafkaSourceConsumerState = KafkaSourceConsumerState(None, None)

  /**
    * Performs a poll on kafka
    * Updates the state of the kafka consumer
    *
    * @param cb callback to invoke for every element
    */
  def poll(cb: TElement => Unit): Unit = poll(cb, Map[Int, Long]())

  /**
    * Performs a poll on kafka, up to the given offset
    * Updates the state of the kafka consumer
    *
    * @param cb          callback to notify new elements to
    * @param seekOffsets maximum offsets to retrieve
    */
  def poll(cb: TElement => Unit, seekOffsets: PartialFunction[Int, Long]): Unit = {
    val shouldInclude = (r: ConsumerRecord[TKey, TValue]) =>
      seekOffsets.lift(r.partition()).forall(_ >= r.offset())
    poll(cb, shouldInclude, seekOffsets)
  }

  private def poll(cb: TElement => Unit,
                   shouldInclude: ConsumerRecord[TKey, TValue] => Boolean,
                   seekOffsets: PartialFunction[Int, Long]): Unit = synchronized {

    logger.debug(s"$name started polling")
    val data = consumer.poll(pollTimeout).iterator().asScala
    logger.debug(s"$name completed polling")
    val resetSet = ArrayBuffer[Int]()
    data.foreach(o => {
      if (shouldInclude(o)) {
        cb(transform(o.value(), o.key()))
      } else {
        //If we should not include it, we need to reset the consumer after this poll
        val partition = o.partition()
        if (!resetSet.contains(partition)) {
          resetSet += partition
        }
      }
    })
    resetConsumer(resetSet, seekOffsets)

  }

  /**
    * Updates the state of this sourceConsumer with the new offset
    * Should be called within checkpoint lock
    * Typically called at the end of the poll loop with the new offsets
    * Most code is just to provide debug information of the current state
    * @param newOffsets offsets to update the state with
    */
  def updateOffsetState(): Unit = synchronized {
    //Obtain current assignment
    val assignment = consumer.assignment().asScala

    if (assignment.isEmpty) {
      logger.debug(
        s"Not setting offset because assignment was empty in $getLabel. Tried $emptyAssignments times")
      emptyAssignments += 1
    } else {
      //Build offset collection
      val newOffsets = assignment
        .map(o => o -> consumer.position(o))
        .toMap

      logger.debug(s"New offsets in $getLabel: $newOffsets")
      state = KafkaSourceConsumerState(Some(assignment), Some(newOffsets))
      logger.debug(s"New state in $name: $state")
    }
  }

  /**
    * Updates the assignment of topicPartitions
    * In the case of new partitions, assigns -1 as starting offset
    * Typically called after updateOffsetState, within the same lock
    */
  private def updateAssignments(): Unit = synchronized {
    if (newPartitions.nonEmpty) {
      logger.debug(s"received new partitions $newPartitions in $getLabel")

      val newOffsets = newPartitions.get
        .filterNot(o => state.getOffsets.keys.toSet.contains(o))
        .map(tp => tp -> Option(consumer.position(tp)).getOrElse(-1l))

      logger.debug(s"$name removing offsets from state ${state.getOffsets.filter(o =>
        !newPartitions.get.map(_.partition()).exists(_ == o._1))}")

      logger.debug(s"$name assigning new offsets $newOffsets")

      //Remove offsets, and add newly assigned partitions to the offset map

      val newStateOffsets =
        state.getOffsets
          .filter(o => newPartitions.get.exists(_ == o._1)) ++ newOffsets

      //Update assignment and offsets
      state = KafkaSourceConsumerState(newPartitions, Some(newStateOffsets))

      //Reset the value
      newPartitions = None
    }
  }

  /**
    * Notify the source consimer of a new assignment
    * @param partitions
    */
  private[source] def onNewAssignment(partitions: Iterable[TopicPartition]): Unit = synchronized {
    logger.info(s"New assignment for $name: $partitions")
    newPartitions = Some(partitions)
  }

  private[source] def resetConsumer(partitions: Seq[Int],
                                    offsets: PartialFunction[Int, Long]): Unit = {
    if (partitions.nonEmpty) {
      logger.debug(s"Perfoming a seek on $name")
      partitions.foreach(partition => {
        val offset = offsets(partition)
        consumer.seek(new TopicPartition(topic, partition), offset)
      })
    }
  }

  /**
    * Sets the position of the consumer to the given offsets
    * Uses the assignment to reset the position
    * Throws an exception if no assignment was made yet
    * TODO: Determine what to do when the assignment changes later during the process, after a call to this method was made
    * @param offsets p offsets to reset to
    */
  def setPosition(offsets: Map[Int, Long]): Unit = synchronized {
    val seekSet = state.assignment match {
      case Some(v) => v.map(o => (o, offsets.lift.apply(o.partition())))
      case none =>
        throw new NotImplementedError(
          "Cannot set position when no assignment has been made yet. Need to implement a handler if this is required")
    }
    seekSet.foreach {
      case (tp, Some(offset)) => consumer.seek(tp, offset)
      case (tp, none) =>
        throw new IllegalStateException(
          s"Cannot set position, because no offset was passed for TP ${tp.topic()}${tp.partition()}.")
    }
    updateOffsetState()
  }

  /**
    * Commits the passed offsets
    *
    * @param offsets the offsets to commit
    */
  def commit(offsets: Map[Int, Long]): Unit = synchronized {
    logger.debug(s"Committing offsets $offsets")
    val commitData =
      offsets.map(o => (new TopicPartition(topic, o._1), new OffsetAndMetadata(o._2))).asJava
    consumer.commitSync(commitData)
  }

  /**
    * Obtains the offset currently stored in the consumer state.
    * Used to initialize a kafkaSource
    *
    * @return
    */
  def getCurrentOffsets: Map[Int, Long] = state.getOffsets.map(o => (o._1.partition(), o._2))

  /**
    * Retrieves the current position on the assigned partitions of the consumer
    *
    * @return

  def getCurrentPosition: mutable.Map[Int, Long] =
    mutable.Map[Int, Long]() ++= consumer
      .assignment()
      .asScala
      .map(o => o.partition() -> consumer.position(o))
    */
  /**
    * Retrieves kafka endoffsets -1. (Kafka returns last avalaible message + 1)
    *
    * @return
    */
  def getEndOffsets: Map[Int, Long] =
    consumer
      .endOffsets(consumer.assignment())
      .asScala
      .map(o => o._1.partition() -> (o._2.toLong - 1))
      .toMap

  /**
    * Returns if the current offsets has passed the reference offset
    *
    * @param reference offsets to compare to
    * @return None if no assignment has ever been made to this consumer
    *         True if the consumer is further than the reference for all assigned partitions
    *         False if the consumer is behind on one or more assigned partitions
    */
  def higherOrEqual(reference: PartialFunction[Int, Long]): Option[Boolean] =
    state.offsets.map(offset =>
      offset.forall(o => hasPassedReference(reference)(o._1.partition(), o._2)))

  private def hasPassedReference(
      referenceSet: PartialFunction[Int, Long])(partition: Int, currentOffset: Long): Boolean =
    referenceSet
      .lift(partition) match {
      case Some(v) => v < currentOffset
      case None => true
    }

  /**
    * Closes the kafka consumer
    */
  def close(): Unit = {
    consumer.commitSync()
    consumer.close()
  }
}

/**
  * Companion object for Kafka Source consumer, with initialization logic
  */
object KafkaSourceConsumer {

  /**
    * Construct a new KafkaSourceConsumer
    *
    * @param sourceUuid      unique identifier of the Flink source that uses this consumer
    * @param consumerFactory factory providing kafka consumers
    * @tparam TElement Type of the element provided by the consumer
    * @tparam TValue   Type of the value in Kafka
    * @tparam TKey     Type of key in Kafka
    * @return
    */
  def apply[TElement, TValue: ClassTag, TKey: ClassTag](name: String,
                                                        sourceUuid: String,
                                                        topic: String)(
      mapper: KafkaSourceMapper[TElement, TValue, TKey])(consumerFactory: KafkaConsumerFactory) = {
    val kafkaConsumer = consumerFactory.create[TKey, TValue](sourceUuid)
    //TODO: Subscribe
    val rebalanceListener = new RebalanceListenerImpl()
    kafkaConsumer.subscribe(Iterable(topic).asJavaCollection, rebalanceListener)

    val sourceConsumer =
      new KafkaSourceConsumer[TElement, TValue, TKey](name, topic, kafkaConsumer)
      with KafkaSourceMapper[TElement, TValue, TKey] {
        override def transform(value: TValue, key: TKey): TElement = mapper.transform(value, key)
      }

    wire(rebalanceListener, sourceConsumer)
    sourceConsumer
  }

  /**
    * Wires up the observables to the kafkaConsumer to update it's partitions as the events come in
    *
    * @param observable    consumer rebalance observable, passing all rebalance events
    * @param kafkaConsumer the consumer to wire
    * @tparam TElement Type of the element provided by the consumer
    * @tparam TValue   Type of the value in Kafka
    * @tparam TKey     Type of key in Kafka
    */
  private def wire[TElement, TValue, TKey](
      observable: ConsumerRebalanceObservable,
      kafkaConsumer: KafkaSourceConsumer[TElement, TValue, TKey]) {
    //TODO: Wire on reassignment of partitions is disabled for now, because we probably don't need it.
    //TODO: Remove implementation in the future
    //observable.observePartitions().subscribe(kafkaConsumer.onNewAssignment(_))
  }

}
