package org.codefeedr.core.library.internal.kafka.source

import java.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.runtime.state.FunctionSnapshotContext
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.types.Row
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition
import org.codefeedr.model.{RecordSourceTrail, TrailedRecord}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Class performing the polling of kafka for a KafkaSource
  * Converting (java)types we use on Kafka back to (scala) types we use in Flink
  */
class KafkaSourceConsumer[T](
                              name:String,
                              consumer: KafkaConsumer[RecordSourceTrail,Row],
                              mapper: TrailedRecord => T) extends LazyLogging{

  //Timeout when polling kafka
  @transient private lazy val pollTimeout = 1000

  /**
    * Performs a poll on kafka
    * @param ctx
    * @return the last offsets for each partition of the consumer that has been collected in the poll
    */
  def poll(ctx: SourceFunction.SourceContext[T]):Map[Int, Long] = {
    logger.debug(s"$name started polling")
    val data = consumer.poll(pollTimeout).iterator().asScala
    logger.debug(s"$name completed polling")
    val r = mutable.Map[Int, Long]()

    data.foreach(o => {
      ctx.collect(mapper(TrailedRecord(o.value())))
      r(o.partition()) = o.offset()
    })
    r.toMap
  }

  /**
    * Commits the passed offsets
    * @param offsets
    */
  def commit(offsets: Map[Int, Long]): Unit = {
    //TODO: Implement
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
  def getCurrentOffsets(): mutable.Map[Int, Long] =
    mutable.Map[Int, Long]() ++= consumer.assignment().asScala.map(o => o.partition() -> consumer.position(o))


  /**
    * Closes the kafka consumer
    */
  def close(): Unit = {
    consumer.close()
  }
}
