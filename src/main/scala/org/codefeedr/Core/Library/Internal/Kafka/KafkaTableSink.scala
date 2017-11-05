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

import java.lang
import java.util.UUID

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.table.sinks.{AppendStreamTableSink, RetractStreamTableSink, TableSink}
import org.apache.flink.types.Row
import org.codefeedr.Core.Library.Internal.{KeyFactory, SubjectTypeFactory}
import org.codefeedr.Core.Library.{LibraryServices, SubjectFactory, TypeInformationServices}
import org.codefeedr.Model._
import scala.concurrent.duration._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class KafkaTableSink(subjectName: String, subjectType: SubjectType)
    extends RetractStreamTableSink[Row] {
  @transient lazy val sink: SinkFunction[tuple.Tuple2[lang.Boolean, Row]] =
    SubjectFactory.GetRowSink(subjectType)

  override def emitDataStream(dataStream: DataStream[tuple.Tuple2[lang.Boolean, Row]]): Unit =
    dataStream
      .addSink(sink)

  override def getFieldNames: Array[String] = subjectType.properties.map(o => o.name)

  override def getFieldTypes: Array[TypeInformation[_]] =
    subjectType.properties.map(o => o.propertyType)

  /**
    * Constructs a new KafkaTableSink with a newly generated subject with the given fields
    * @param fieldNames
    * @param fieldTypes
    * @return
    */
  override def configure(
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]]): TableSink[tuple.Tuple2[lang.Boolean, Row]] = {
    KafkaTableSink(subjectName, fieldNames, fieldTypes)
  }

  override def getRecordType: TypeInformation[Row] =
    TypeInformationServices.GetRowTypeInfo(subjectType)

}

object KafkaTableSink extends LibraryServices {
  def apply(subjectName: String,
            fieldNames: Array[String],
            fieldTypes: Array[TypeInformation[_]]): KafkaTableSink = {
    val subjectFuture = subjectLibrary.GetOrCreateType(
      subjectName,
      () => SubjectTypeFactory.getSubjectType(subjectName, fieldNames, fieldTypes))
    //Have to implement the non-async FLINK api, thus must block here
    val subjectType =
      Await.ready[SubjectType](subjectFuture, Duration(5000, MILLISECONDS)).value.get.get
    new KafkaTableSink(subjectName, subjectType)
  }

  def apply(subjectName: String): KafkaTableSink = new KafkaTableSink(subjectName, null)
}
