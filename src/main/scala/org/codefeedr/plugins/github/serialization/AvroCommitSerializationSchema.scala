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

import java.io.ByteArrayOutputStream
import java.util

import com.sksamuel.avro4s._
import io.confluent.kafka.schemaregistry.client.{
  CachedSchemaRegistryClient,
  SchemaMetadata,
  SchemaRegistryClient
}
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.flink.api.common.serialization.SerializationSchema
import org.codefeedr.plugins.github.clients.GitHubProtocol._

class AvroCommitSerializationSchema(topic: String) extends SerializationSchema[Commit] {

  @transient
  lazy val schemaRegistry: SchemaRegistryClient = {
    val registry = new CachedSchemaRegistryClient("http://127.0.0.1:8081", 1000)

    val schema = AvroSchema[Commit]
    registry.register(topic + "-value", schema)
    registry
  }

  @transient
  lazy val avroSerializer: KafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistry)

  override def serialize(element: Commit): Array[Byte] = {
    val format = RecordFormat[Commit]
    println(topic)
    println(element)
    avroSerializer.serialize(topic, format.to(element))
  }

}
