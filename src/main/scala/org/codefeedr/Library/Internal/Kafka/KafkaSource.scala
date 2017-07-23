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

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.codefeedr.Library.Internal.Bagger
import org.codefeedr.Model.{Record, RecordIdentifier, SubjectType}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

/**
  * Created by Niels on 18/07/2017.
  */
class KafkaSource[TData: TypeInformation: ru.TypeTag: ClassTag](subjectType: SubjectType)
    extends RichSourceFunction[TData]
    with ResultTypeQueryable[TData]
    with LazyLogging {

  @transient private lazy val dataConsumer = {
    val consumer = KafkaConsumerFactory.create[Array[Byte], Record](uuid.toString)
    consumer.subscribe(Iterable(topic).asJavaCollection)
    logger.debug(s"Source $uuid subsribed on topic $topic as group")
    consumer
  }
  @transient private lazy val topic = s"${subjectType.name}_${subjectType.uuid}"

  @transient private lazy val bagger = new Bagger[TData]()

  @transient private lazy val uuid = UUID.randomUUID()

  //Make this configurable?
  @transient private lazy val RefreshTime = 100
  @transient private lazy val PollTimeout = 1000

  @transient private var running = false

  override def cancel(): Unit = {
    running = false
  }

  override def run(ctx: SourceFunction.SourceContext[TData]): Unit = {
    running = true
    while (running) {
      dataConsumer
        .poll(PollTimeout)
        .iterator()
        .asScala
        .map(o => {
          val record = o.value()
          bagger.Unbag(record)
        })
        .foreach(ctx.collect)
      Thread.sleep(RefreshTime)
    }
  }

  override def getProducedType: TypeInformation[TData] = createTypeInformation[TData]
}
