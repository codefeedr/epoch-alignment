package org.codefeedr.Library.Internal

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util

/**
  * Created by Niels on 14/07/2017.
  */
class KafkaDeserializer[T] extends org.apache.kafka.common.serialization.Deserializer[T] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): T = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(data))
    val value = ois.readObject()
    ois.close()
    value.asInstanceOf[T]
  }
}
