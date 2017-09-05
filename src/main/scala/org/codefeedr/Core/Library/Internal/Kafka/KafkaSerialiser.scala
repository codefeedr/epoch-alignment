/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
