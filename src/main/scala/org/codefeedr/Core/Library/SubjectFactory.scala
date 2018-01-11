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

package org.codefeedr.Core.Library

import java.util.UUID

import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.types.Row
import org.codefeedr.Core.Library.Internal.Kafka.Sink.{KafkaGenericSink, RowSink, TrailedRecordSink}
import org.codefeedr.Core.Library.Internal.Kafka.Source.KafkaRowSource
import org.codefeedr.Core.Library.Internal.Kafka._
import org.codefeedr.Core.Library.Internal.{KeyFactory, RecordTransformer}
import org.codefeedr.Model.{ActionType, SubjectType, TrailedRecord}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

/**
  * ThreadSafe
  * Created by Niels on 18/07/2017.
  */
class SubjectFactoryController { this: LibraryServices =>
  def GetSink[TData: ru.TypeTag: ClassTag]: Future[SinkFunction[TData]] = {
    subjectLibrary
      .GetOrCreateType[TData]()
      .flatMap(o =>
        KafkaController.GuaranteeTopic(s"${o.name}_${o.uuid}").map(_ => new KafkaGenericSink(o)))
  }

  /**
    * Get a generic sink for the given type
    * @param subjectType
    * @return
    */
  def GetSink(subjectType: SubjectType): SinkFunction[TrailedRecord] =
    new TrailedRecordSink(subjectType)

  /**
    * Return a sink for the tableApi
    * @param subjectType
    * @return
    */
  def GetRowSink(subjectType: SubjectType) = new RowSink(subjectType)

  /**
    * Construct a serializable and distributable mapper function from any source type to a TrailedRecord
    * @tparam TData Type of the source object to get a mapper for
    * @return A function that can convert the object into a trailed record
    */
  def GetTransformer[TData: ru.TypeTag: ClassTag](
      subjectType: SubjectType): TData => TrailedRecord = {
    @transient lazy val transformer = new RecordTransformer[TData](subjectType)
    @transient lazy val keyFactory = new KeyFactory(subjectType, UUID.randomUUID())
    (d: TData) =>
      {
        val record = transformer.Bag(d, ActionType.Add)
        val trail = keyFactory.GetKey(record)
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
  def GetUnTransformer[TData: ru.TypeTag: ClassTag](
      subjectType: SubjectType): TrailedRecord => TData = { (r: TrailedRecord) =>
    {
      val transformer = new RecordTransformer[TData](subjectType)
      transformer.Unbag(r)
    }
  }

  def GetRowSource(subjectType: SubjectType): SourceFunction[Row] = {
    new KafkaRowSource(subjectType)
  }

  def GetSource(subjectType: SubjectType): SourceFunction[TrailedRecord] = {
    new KafkaTrailedRecordSource(subjectType)
  }

}
// with LibraryServices
object SubjectFactory extends SubjectFactoryController with LibraryServices {}
