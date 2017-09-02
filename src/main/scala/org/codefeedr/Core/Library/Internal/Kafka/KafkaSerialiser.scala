

package org.codefeedr.Core.Library.Internal.Kafka

import java.util

import org.codefeedr.Core.Library.Internal.Serialisation.GenericSerialiser

import scala.reflect.ClassTag

/**
  * Created by Niels on 14/07/2017.
  */
class KafkaSerialiser[T: ClassTag] extends org.apache.kafka.common.serialization.Serializer[T] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  private lazy val GenericSerialiser = new GenericSerialiser[T]()

  override def serialize(topic: String, data: T): Array[Byte] = GenericSerialiser.Serialize(data)

  override def close(): Unit = {}
}
