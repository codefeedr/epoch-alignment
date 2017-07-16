package org.codefeedr.Library.Internal

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util

/**
  * Created by Niels on 14/07/2017.
  */
class KafkaSerializer[T] extends org.apache.kafka.common.serialization.Serializer[T] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def serialize(topic: String, data: T): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(data)
    oos.close
    stream.toByteArray
  }

  override def close(): Unit = {}
}
