

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

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.kafka.clients.producer.ProducerRecord
import org.codefeedr.Model._

/**
  * A simple kafka sink, pushing all records to a kafka topic of the given subjecttype
  * Not thread safe
  * Serializable (with lazy initialisation)
  * Created by Niels on 11/07/2017.
  */
class KafkaSink(subjectType: SubjectType)
    extends RichSinkFunction[TrailedRecord]
    with LazyLogging
    with Serializable {
  @transient private lazy val kafkaProducer = {
    val producer = KafkaProducerFactory.create[RecordSourceTrail, Record]
    logger.debug(s"Producer $uuid created for topic $topic")
    producer
  }

  @transient private lazy val topic = s"${subjectType.name}_${subjectType.uuid}"
  //A random identifier for this specific sink
  @transient private lazy val uuid = UUID.randomUUID()

  override def close(): Unit = {
    kafkaProducer.close()
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def invoke(trailedRecord: TrailedRecord): Unit = {
    kafkaProducer.send(new ProducerRecord(topic, trailedRecord.trail, trailedRecord.record))
  }
}
