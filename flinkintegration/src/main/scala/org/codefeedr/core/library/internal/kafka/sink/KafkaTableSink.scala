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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.table.sinks.{RetractStreamTableSink, TableSink}
import org.apache.flink.types.Row
import org.codefeedr.core.library.internal.SubjectTypeFactory
import org.codefeedr.core.library.{LibraryServices, TypeInformationServices}
import org.codefeedr.core.library.metastore.{JobNode, SubjectNode}
import org.codefeedr.model._

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, _}

class KafkaTableSink(subjectNode: SubjectNode,
                     jobNode: JobNode,
                     sinkId: String,
                     sink: SinkFunction[tuple.Tuple2[lang.Boolean, Row]])
    extends RetractStreamTableSink[Row] {

  @transient lazy val subjectType = subjectNode.getDataSync().get

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
    KafkaTableSink(subjectNode.name, jobNode.name, fieldNames, fieldTypes, sinkId)
  }

  override def getRecordType: TypeInformation[Row] =
    TypeInformationServices.getRowTypeInfo(subjectType)

}

object KafkaTableSink {
  def apply(subjectName: String,
            jobName: String,
            fieldNames: Array[String],
            fieldTypes: Array[TypeInformation[_]],
            sinkId: String): KafkaTableSink = {
    val subjectNode = LibraryServices.subjectLibrary
      .getSubject(subjectName)
    val jobNode = LibraryServices.subjectLibrary.getJob(jobName)

    val subjectFuture = subjectNode.getOrCreate(() =>
      SubjectTypeFactory.getSubjectType(subjectName, fieldNames, fieldTypes))
    val subjectType =
      Await.ready[SubjectType](subjectFuture, Duration(5000, MILLISECONDS)).value.get.get
    val rowSink = LibraryServices.subjectFactory.getRowSink(subjectType, jobName, sinkId)
    //Have to implement the non-async FLINK api, thus must block here
    new KafkaTableSink(subjectNode, jobNode, sinkId, rowSink)
  }

  def apply(subjectName: String, jobName: String, sinkId: String): KafkaTableSink = {
    val subjectNode = LibraryServices.subjectLibrary.getSubject(subjectName)
    val jobNode = LibraryServices.subjectLibrary.getJob(jobName)
    new KafkaTableSink(subjectNode, jobNode, sinkId, null)
  }
}
