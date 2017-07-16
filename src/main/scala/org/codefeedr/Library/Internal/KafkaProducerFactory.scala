package org.codefeedr.Library.Internal

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.Serializer

/**
  * Created by Niels on 11/07/2017.
  */
object KafkaProducerFactory {

  /**
    * Create a kafkaproducer for a specific data and key type
    * @tparam TData Type of the data object
    * @tparam TKey Type of the key idintifying the data object
    * @return A kafka producer cabapble of pushing the tuples to kafka
    */
  def Create[TKey, TData]: KafkaProducer[TKey, TData] =
    new KafkaProducer[TKey, TData](KafkaConfig.properties,
                                   new KafkaSerializer[TKey],
                                   new KafkaSerializer[TData])
}
