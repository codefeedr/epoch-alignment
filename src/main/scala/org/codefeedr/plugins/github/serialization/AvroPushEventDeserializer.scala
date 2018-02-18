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
package org.codefeedr.plugins.github.serialization

import com.sksamuel.avro4s.{AvroSchema, RecordFormat, SchemaFor}
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{TypeExtractionUtils, TypeExtractor}
import org.codefeedr.plugins.github.clients.GitHubProtocol.PushEvent

class AvroPushEventDeserializer(topic: String) extends DeserializationSchema[PushEvent] {

  @transient
  private lazy val schemaRegistry: SchemaRegistryClient = {
    val registry = new CachedSchemaRegistryClient("http://127.0.0.1:8081", 1000)
    val subject = topic + "-value"
    val schema = AvroSchema[PushEvent]

    if (!registry.getAllSubjects.contains(subject)) {
      registry.register(subject, schema)
    }

    registry
  }

  @transient
  private lazy val schema = AvroSchema[PushEvent]

  @transient
  lazy val avroSerializer: KafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistry)

  override def isEndOfStream(nextElement: PushEvent): Boolean = {
    false
  }

  override def deserialize(message: Array[Byte]): PushEvent = {
    val format = RecordFormat[PushEvent]
    format.from(avroSerializer.deserialize(topic, message, schema).asInstanceOf[GenericRecord])
  }

  override def getProducedType: TypeInformation[PushEvent] = {
    TypeExtractor.createTypeInfo(classOf[PushEvent])
  }
}
