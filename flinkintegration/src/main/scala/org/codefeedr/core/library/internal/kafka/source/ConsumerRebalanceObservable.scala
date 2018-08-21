package org.codefeedr.core.library.internal.kafka.source

import java.util

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import rx.lang.scala.{Observable, Subject}
import rx.lang.scala.subjects.ReplaySubject
import scala.collection.JavaConverters._
/**
  * Rx Wrapper around rebalance events from Kafka
  */
trait ConsumerRebalanceObservable {
  protected lazy val partitionsRevoked:Subject[Iterable[TopicPartition]] = ReplaySubject[Iterable[TopicPartition]]()
  protected lazy val partitionsAssigned:Subject[Iterable[TopicPartition]] = ReplaySubject[Iterable[TopicPartition]]()

  /**
    * Observe all topic partitions removed from the assignment
    * @return
    */
  def observePartitionsRevoked():Observable[Iterable[TopicPartition]] = partitionsRevoked

  /**
    * Observe all topic partitions added to the assignment
    * @return
    */
  def observePartitionsAssigned():Observable[Iterable[TopicPartition]] = partitionsAssigned

  /**
    * Observe the currently assigned partitions, with an event for each modification
    * @return
    */
  def observePartitions(): Observable[Iterable[TopicPartition]] =
    partitionsRevoked.map(revoke)
    .merge(partitionsAssigned.map(assign))
    .foldLeft(Iterable.empty[TopicPartition])((s,v) => {
      v match {
        case revoke(tp) => s.filter(r => !tp.exists(o => o.equals(r)))
        case assign(tp) => s ++ tp
      }
    })



  trait assignmentEvent{}
  case class revoke(tp: Iterable[TopicPartition]) extends assignmentEvent
  case class assign(tp: Iterable[TopicPartition]) extends assignmentEvent
}


/**
  * Rx Wrapper around rebalance events from kafka
  */
class RebalanceListenerImpl extends ConsumerRebalanceListener with ConsumerRebalanceObservable
{
  override protected def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = partitionsRevoked.onNext(partitions.asScala)

  override protected def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = partitionsAssigned.onNext(partitions.asScala)
}
