package org.codefeedr.Library.Internal

import org.apache.kafka.clients.consumer._

/**
  * Created by Niels on 14/07/2017.
  */
object KafkaConsumerFactory {
  def create[TData, TKey](): KafkaConsumer[TData, TKey] = {
    new KafkaConsumer[TData, TKey](KafkaConfig.properties)
  }
}
