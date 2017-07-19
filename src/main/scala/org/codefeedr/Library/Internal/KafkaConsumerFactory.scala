package org.codefeedr.Library.Internal

import java.util.UUID

import org.apache.kafka.clients.consumer._

/**
  * Created by Niels on 14/07/2017.
  */
object KafkaConsumerFactory {

  /**
    * Create a new kafka consumer
    * @param group Groupname. Each group recieves each value once
    * @tparam TKey
    * @tparam TData
    * @return
    */
  def create[TKey, TData](group: String): KafkaConsumer[TKey, TData] = {
    val properties = KafkaConfig.consumerPropertes
    properties.setProperty("group.id", group)
    new KafkaConsumer[TKey, TData](properties,
                                   new KafkaDeserializer[TKey],
                                   new KafkaDeserializer[TData])
  }
}
