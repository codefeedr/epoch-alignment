package org.codefeedr.Library.Internal

import org.apache.kafka.clients.producer.KafkaProducer

/**
  * Created by Niels on 11/07/2017.
  */
object KafkaProducerFactory {
  def Create[TData, TKey]: KafkaProducer[TData, TKey] =
    new KafkaProducer[TData, TKey](KafkaConfig.properties)
}
