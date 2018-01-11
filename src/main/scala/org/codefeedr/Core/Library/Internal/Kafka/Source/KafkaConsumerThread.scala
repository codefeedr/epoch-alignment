package org.codefeedr.Core.Library.Internal.Kafka.Source

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.types.Row
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.codefeedr.Model.{RecordSourceTrail, TrailedRecord}
import java.util.UUID.randomUUID
import scala.collection.JavaConverters._

/**
  * Thread that handles polling of data from kafka partitions
  * Not serializable, so create this thread inside the job
  */
class KafkaConsumerThread(kafkaConsumer: KafkaConsumer[RecordSourceTrail, Row],name:String)
    extends Runnable
    with LazyLogging {

  private val uuid = randomUUID().toString
  @transient private lazy val PollTimeout = 1000

  //Variable that will contain the data once poll has completed
  private var data : List[TrailedRecord] = null

  def GetData(): List[TrailedRecord] = data

  override def run(): Unit = {
    logger.debug(s"$name Polling")

    data = kafkaConsumer
      .poll(PollTimeout)
      .iterator()
      .asScala
      .map(o => TrailedRecord(o.value()))
      .toList

    logger.debug(s"$name completed poll")
  }
}
