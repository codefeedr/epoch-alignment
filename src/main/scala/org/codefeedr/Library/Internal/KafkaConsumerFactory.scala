package org.codefeedr.Library.Internal

import org.apache.kafka.clients.consumer._

/**
  * Created by Niels on 14/07/2017.
  */
object KafkaConsumerFactory {
  def create[TKey, TData](): KafkaConsumer[TKey, TData] = {
    new KafkaConsumer[TKey, TData](KafkaConfig.consumerPropertes,
                                   new KafkaDeserializer[TKey],
                                   new KafkaDeserializer[TData])
  }
}
