package org.codefeedr.Library.Internal

import java.util.{Properties, UUID}

import org.apache.kafka.clients.consumer._

/**
  * Created by Niels on 14/07/2017.
  */
object KafkaConsumerFactory {

  /**
    * Create a new kafka consumer
    * @param group Groupname. Each group recieves each value once
    * @tparam TKey Type of the key used by kafka
    * @tparam TData Data type used to send to kafka
    * @return
    */
  def create[TKey, TData](group: String): KafkaConsumer[TKey, TData] = {
    //Kafka consumer constructor is not thread safe!
    val properties = KafkaConfig.consumerPropertes.clone().asInstanceOf[Properties]
    properties.setProperty("group.id", group)
    new KafkaConsumer[TKey, TData](properties,
                                   new KafkaDeserializer[TKey],
                                   new KafkaDeserializer[TData])

  }
}
