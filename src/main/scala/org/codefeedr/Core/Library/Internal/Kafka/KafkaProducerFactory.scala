

package org.codefeedr.Core.Library.Internal.Kafka

import org.apache.kafka.clients.producer.KafkaProducer

import scala.reflect.ClassTag

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
  def create[TKey: ClassTag, TData: ClassTag]: KafkaProducer[TKey, TData] =
    new KafkaProducer[TKey, TData](KafkaConfig.properties,
                                   new KafkaSerialiser[TKey],
                                   new KafkaSerialiser[TData])
}
