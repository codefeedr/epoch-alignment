/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.codefeedr.Library.Internal.Kafka

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util
import scala.reflect._

/**
  * Created by Niels on 14/07/2017.
  */
class KafkaDeserializer[T: ClassTag](implicit ct: ClassTag[T])
    extends org.apache.kafka.common.serialization.Deserializer[T] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  /**
    * Prevent deserialisation of something that already is a byte array, as those are also not serialized
    */
  private val deserializeInternal =
    if (classOf[Array[Byte]].isAssignableFrom(ct.getClass)) { (data: Array[Byte]) =>
      data.asInstanceOf[T]
    } else { (data: Array[Byte]) =>
      {
        val ois = new ObjectInputStream(new ByteArrayInputStream(data))
        val value = ois.readObject()
        ois.close()
        value.asInstanceOf[T]
      }
    }

  override def deserialize(topic: String, data: Array[Byte]): T = deserializeInternal(data)
}
