package org.codefeedr.Library.Internal

import java.util.Properties

/**
  * Created by Niels on 11/07/2017.
  */
object KafkaConfig {
  //This should be moved to some sort of configuration file
  @transient lazy val properties: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("retries", Predef.int2Integer(0))
    props.put("batch.size", Predef.int2Integer(16384))
    props.put("linger.ms", Predef.int2Integer(1))
    props.put("buffer.memory", Predef.int2Integer(33554432))
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props
  }
}
