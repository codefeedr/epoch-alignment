package org.codefeedr.Library.Internal.Kafka

import org.apache.kafka.clients.producer.KafkaProducer

/**
  * Created by Niels on 11/07/2017.
  */
object KafkaProducerFactory {

  /**
    * Create a kafka producer for a specific data and key type
    * @tparam TData Type of the data object
    * @tparam TKey Type of the key identifying the data object
    * @return A kafka producer capable of pushing the tuple to kafka
    */
  def create[TKey, TData]: KafkaProducer[TKey, TData] =
    new KafkaProducer[TKey, TData](KafkaConfig.properties,
                                   new KafkaSerializer[TKey],
                                   new KafkaSerializer[TData])
}
