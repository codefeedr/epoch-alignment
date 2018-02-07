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

package org.codefeedr.core.library.internal.kafka.sink

import java.lang
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.java.tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.types.Row
import org.apache.kafka.clients.producer.ProducerRecord
import org.codefeedr.core.library.internal.KeyFactory
import org.codefeedr.core.library.LibraryServices
import org.codefeedr.core.library.metastore.{ProducerNode, QuerySinkNode, SubjectNode}
import org.codefeedr.model.zookeeper.{Producer, QuerySink}
import org.codefeedr.model._

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * A simple kafka sink, pushing all records to a kafka topic of the given subjecttype
  * Not thread safe
  * Because this class needs to be serializable and the LibraryServices are not, no dependency injection structure can be used here :(
  * Serializable (with lazy initialisation)
  * Created by Niels on 11/07/2017.
  */
abstract class KafkaSink[TSink]
    extends RichSinkFunction[TSink]
    with LazyLogging
    with Serializable
    with LibraryServices {

  @transient protected lazy val kafkaProducer = {
    val producer = KafkaProducerFactory.create[RecordSourceTrail, Row]
    //Create the producer node alongside
    //Need to block here because the producer cannot start before the zookeeper state has been configured
    Await.ready(subjectNode.create(subjectType), Duration(5, SECONDS))
    Await.ready(sinkNode.create(QuerySink(sinkUuid)), Duration(5, SECONDS))
    Await.ready(producerNode.create(Producer(instanceUuid, null, System.currentTimeMillis())),
                Duration(5, SECONDS))

    logger.debug(s"Producer ${GetLabel()} created for topic $topic")
    producer
  }

  val subjectType: SubjectType
  val sinkUuid: String

  @transient lazy val subjectNode: SubjectNode = subjectLibrary.getSubject(subjectType.name)

  @transient lazy val sinkNode: QuerySinkNode = subjectNode.getSinks().getChild(sinkUuid)

  /**
    * The ZookeeperNode that represent this instance of the producer
    */
  @transient lazy val producerNode: ProducerNode = sinkNode.getProducers().getChild(instanceUuid)

  @transient protected lazy val topic = s"${subjectType.name}_${subjectType.uuid}"
  //A random identifier for this specific sink
  @transient lazy val instanceUuid = UUID.randomUUID().toString

  def GetLabel(): String = s"KafkaSink ${subjectType.name}(${sinkUuid}-${instanceUuid})"

  override def close(): Unit = {
    logger.debug(s"Closing producer ${GetLabel()}for ${subjectType.name}")
    kafkaProducer.close()
    Await.ready(producerNode.setState(false), Duration.Inf)
  }

  override def open(parameters: Configuration): Unit = {
    kafkaProducer
    logger.debug(s"Opening producer ${GetLabel()} for ${subjectType.name}")
    Await.ready(producerNode.setState(true), Duration.Inf)
  }
}

class TrailedRecordSink(override val subjectType: SubjectType, override val sinkUuid: String)
    extends KafkaSink[TrailedRecord] {
    override def invoke(trailedRecord: TrailedRecord): Unit = {
    logger.debug(s"Producer ${GetLabel}sending a message to topic $topic")
    kafkaProducer.send(new ProducerRecord(topic, trailedRecord.trail, trailedRecord.row))

      kafkaProducer.beginTransaction()

  }
}



class RowSink(override val subjectType: SubjectType, override val sinkUuid: String)
    extends KafkaSink[tuple.Tuple2[lang.Boolean, Row]] {
  @transient lazy val keyFactory = new KeyFactory(subjectType, UUID.randomUUID())

  override def invoke(value: tuple.Tuple2[lang.Boolean, Row]): Unit = {
    logger.debug(s"Producer ${GetLabel} sending a message to topic $topic")
    val actionType = if (value.f0) ActionType.Add else ActionType.Remove
    //TODO: Optimize these steps
    val record = Record(value.f1, subjectType.uuid, actionType)
    val trailed = TrailedRecord(record, keyFactory.getKey(record))
    kafkaProducer.send(new ProducerRecord(topic, trailed.trail, trailed.row))
  }
}
