package org.codefeedr.Library

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.codefeedr.Library.Internal.KafkaProducerFactory

/**
  * Created by Niels on 11/07/2017.
  */
class KafkaSink[TData] extends RichSinkFunction[TData] {
  @transient lazy val kafkaProducer = KafkaProducerFactory.Create

  override def invoke(value: TData): Unit = {

  }
}
