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

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.table.sinks.{AppendStreamTableSink, TableSink}
import org.apache.flink.types.Row
import org.codefeedr.Core.Library.{SubjectFactory, TypeInformationServices}
import org.codefeedr.Model.{SubjectType, TrailedRecord}

class KafkaTableSink(subjectType: SubjectType) extends AppendStreamTableSink[Row] {
  @transient lazy val sink: SinkFunction[TrailedRecord] = SubjectFactory.GetSink(subjectType)

  override def emitDataStream(dataStream: DataStream[Row]): Unit = {
    dataStream
      .map(
        new MapFunction[Row, TrailedRecord]() {
          override def map(value: Row): TrailedRecord = TrailedRecord(value)
        }
      )
      .addSink(sink)
  }

  override def getOutputType: TypeInformation[Row] =
    TypeInformationServices.GetRowTypeInfo(subjectType)

  override def getFieldNames: Array[String] = subjectType.properties.map(o => o.name)

  override def getFieldTypes: Array[TypeInformation[_]] =
    subjectType.properties.map(o => o.propertyType)

  /**
    * TODO: Check if this can actually be completely ignored
    * Our own wrapper should ensure this information is already present at the moment the tablesink is created
    * @param fieldNames
    * @param fieldTypes
    * @return
    */
  override def configure(fieldNames: Array[String],
                         fieldTypes: Array[TypeInformation[_]]): TableSink[Row] = {
    this
  }
}
