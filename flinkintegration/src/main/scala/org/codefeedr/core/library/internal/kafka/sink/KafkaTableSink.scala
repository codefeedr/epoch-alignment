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
import org.codefeedr.core.library.metastore.{JobNode, SubjectLibraryComponent, SubjectNode}
import org.codefeedr.core.library.{SubjectFactoryComponent, TypeInformationServices}
import org.codefeedr.model._

import scala.concurrent.Await
import scala.concurrent.duration._

class KafkaTableSink(subjectNode: SubjectNode,
                     jobNode: JobNode,
                     sinkId: String,
                     sink: SinkFunction[tuple.Tuple2[lang.Boolean, Row]])(implicit val factory:KafkaTableSinkFactory)
    extends RetractStreamTableSink[Row] {

  @transient lazy val subjectType: SubjectType = subjectNode.getDataSync().get

  override def emitDataStream(dataStream: DataStream[tuple.Tuple2[lang.Boolean, Row]]): Unit =
    dataStream
      .addSink(sink)

  override def getFieldNames: Array[String] = subjectType.properties.map(o => o.name)

  override def getFieldTypes: Array[TypeInformation[_]] =
    subjectType.properties.map(o => o.propertyType)

  /**
    * Constructs a new KafkaTableSink with a newly generated subject with the given fields
    * @param fieldNames names of the fields of the subject
    * @param fieldTypes types of the fields of the subject
    * @return
    */
  override def configure(
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]]): TableSink[tuple.Tuple2[lang.Boolean, Row]] = {
    factory.create(subjectNode.name, jobNode.name, fieldNames, fieldTypes, sinkId)
  }

  override def getRecordType: TypeInformation[Row] =
    TypeInformationServices.getRowTypeInfo(subjectType)

}

trait KafkaTableSinkFactory {

  /**
    * Creates a new KafkaTableSink
    * Also creates the kafka topic and zookeeper subject for the subject
    *
    * @param subjectName name of the subject to create
    * @param jobName     name of the job the sink is part of
    * @param fieldNames  field names of the subject
    * @param fieldTypes  field types of the subject
    * @param sinkId      unique identifier of this sink
    * @return
    */
  def create(subjectName: String,
             jobName: String,
             fieldNames: Array[String],
             fieldTypes: Array[TypeInformation[_]],
             sinkId: String): KafkaTableSink

  def create(subjectName: String, jobName: String, sinkId: String): KafkaTableSink
}


trait KafkaTableSinkFactoryComponent {
  this:SubjectLibraryComponent
  with SubjectFactoryComponent =>

  implicit val kafkaTableSinkFactory:KafkaTableSinkFactory

  class KafkaTableSinkFactoryImpl extends KafkaTableSinkFactory {

    /**
      * Creates a new KafkaTableSink
      * Also creates the kafka topic and zookeeper subject for the subject
      *
      * @param subjectName name of the subject to create
      * @param jobName     name of the job the sink is part of
      * @param fieldNames  field names of the subject
      * @param fieldTypes  field types of the subject
      * @param sinkId      unique identifier of this sink
      * @return
      */
    override def create(subjectName: String,
                        jobName: String,
                        fieldNames: Array[String],
                        fieldTypes: Array[TypeInformation[_]],
                        sinkId: String): KafkaTableSink = {

      val subjectType = SubjectTypeFactory.getSubjectType(subjectName, fieldNames, fieldTypes)
      val subjectNode = Await.result(subjectFactory.create(subjectType), 5.seconds)
      val jobNode = subjectLibrary.getJob(jobName)

      val rowSink = subjectFactory.getRowSink(subjectType, jobName, sinkId)
      //Have to implement the non-async Flink api, thus must block here
      new KafkaTableSink(subjectNode, jobNode, sinkId, rowSink)
    }

    override def create(subjectName: String, jobName: String, sinkId: String): KafkaTableSink = {
      val subjectNode = subjectLibrary.getSubject(subjectName)
      val jobNode = subjectLibrary.getJob(jobName)
      new KafkaTableSink(subjectNode, jobNode, sinkId, null)
    }
  }

}
