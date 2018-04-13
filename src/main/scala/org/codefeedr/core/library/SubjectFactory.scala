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

package org.codefeedr.core.library

import java.util.UUID

import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.types.Row
import org.codefeedr.core.library.internal.kafka.sink.{
  KafkaGenericSink,
  RowSink,
  TrailedRecordSink
}
import org.codefeedr.core.library.internal.kafka.source.KafkaRowSource
import org.codefeedr.core.library.internal.kafka._
import org.codefeedr.core.library.internal.{KeyFactory, RecordTransformer, SubjectTypeFactory}
import org.codefeedr.core.library.metastore.SubjectNode
import org.codefeedr.model.{ActionType, SubjectType, TrailedRecord}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

/**
  * ThreadSafe
  * Created by Niels on 18/07/2017.
  */
class SubjectFactoryController { this: LibraryServices =>
  def GetSink[TData: ru.TypeTag: ClassTag](sinkId: String): Future[SinkFunction[TData]] = {
    val subjectType = SubjectTypeFactory.getSubjectType[TData]
    subjectLibrary
      .getSubject(subjectType.name)
      .getOrCreate(() => subjectType)
      .flatMap(
        o =>
          KafkaController
            .guaranteeTopic(s"${o.name}_${o.uuid}",
                            conf.getInt("codefeedr.kafka.custom.partition.count"))
            .map(_ => new KafkaGenericSink(o, sinkId)))
  }

  /**
    * Get a generic sink for the given type
    * @param subjectType
    * @return
    */
  def getSink(subjectType: SubjectType, sinkId: String): SinkFunction[TrailedRecord] =
    new TrailedRecordSink(subjectType, sinkId)

  /**
    * Return a sink for the tableApi
    * @param subjectType
    * @return
    */
  def getRowSink(subjectType: SubjectType, sinkId: String) = new RowSink(subjectType, sinkId)

  /**
    * Construct a serializable and distributable mapper function from any source type to a TrailedRecord
    * @tparam TData Type of the source object to get a mapper for
    * @return A function that can convert the object into a trailed record
    */
  def getTransformer[TData: ru.TypeTag: ClassTag](
      subjectType: SubjectType): TData => TrailedRecord = {
    @transient lazy val transformer = new RecordTransformer[TData](subjectType)
    @transient lazy val keyFactory = new KeyFactory(subjectType, UUID.randomUUID())
    (d: TData) =>
      {
        val record = transformer.bag(d, ActionType.Add)
        val trail = keyFactory.getKey(record)
        TrailedRecord(record, trail)
      }
  }

  /**
    * Construct a deserializer to transform a TrailedRecord back into some object
    * Meant to use for development & testing, not very safe
    * @param subjectType The type of the record expected
    * @tparam TData Type of the object to transform to
    * @return The object
    */
  def getUnTransformer[TData: ru.TypeTag: ClassTag](
      subjectType: SubjectType): TrailedRecord => TData = { (r: TrailedRecord) =>
    {
      val transformer = new RecordTransformer[TData](subjectType)
      transformer.unbag(r)
    }
  }

  def getRowSource(subjectNode: SubjectNode, sourceId: String): SourceFunction[Row] = {
    new KafkaRowSource(subjectNode, sourceId)
  }

  def getSource(subjectNode: SubjectNode, sinkId: String): SourceFunction[TrailedRecord] = {
    new KafkaTrailedRecordSource(subjectNode, sinkId)
  }

}
// with LibraryServices
object SubjectFactory extends SubjectFactoryController with LibraryServices {}
