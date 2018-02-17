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

import com.sksamuel.avro4s.{AvroSchema, RecordFormat}
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.flink.api.common.serialization.SerializationSchema
import org.codefeedr.plugins.github.clients.GitHubProtocol.{Commit, PushEvent}

//TODO MAKE THIS GENERIC
class AvroPushEventSerialization(topic: String) extends SerializationSchema[PushEvent] {

  @transient
  private lazy val schemaRegistry: SchemaRegistryClient = {
    val registry = new CachedSchemaRegistryClient("http://127.0.0.1:8081", 1000)

    val schema = AvroSchema[PushEvent]
    registry.register(topic + "-value", schema)
    registry
  }

  @transient
  lazy val avroSerializer: KafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistry)

  override def serialize(element: PushEvent): Array[Byte] = {
    val format = RecordFormat[PushEvent]
    avroSerializer.serialize(topic, format.to(element))
  }

}
