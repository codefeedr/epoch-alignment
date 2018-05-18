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

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.types.Row
import org.codefeedr.core.library.internal.kafka._
import org.codefeedr.core.library.internal.kafka.sink._
import org.codefeedr.core.library.internal.kafka.source.{
  KafkaConsumerFactoryComponent,
  KafkaRowSource
}
import org.codefeedr.core.library.internal.{KeyFactory, RecordTransformer, SubjectTypeFactory}
import org.codefeedr.core.library.metastore.{JobNode, SubjectLibraryComponent, SubjectNode}
import org.codefeedr.model.{ActionType, SubjectType, TrailedRecord}

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

trait SubjectFactoryComponent {
  this: SubjectLibraryComponent
    with ConfigFactoryComponent
    with KafkaProducerFactoryComponent
    with KafkaConsumerFactoryComponent
    with EpochStateManagerComponent =>
  val subjectFactory: SubjectFactoryController

  /**
    * ThreadSafe
    * Created by Niels on 18/07/2017.
    */
  class SubjectFactoryController {
    def GetSink[TData: ru.TypeTag: ClassTag](sinkId: String,
                                             jobName: String): Future[SinkFunction[TData]] = {
      val subjectType = SubjectTypeFactory.getSubjectType[TData]
      val jobNode = subjectLibrary.getJob(jobName)
      val subjectNode = subjectLibrary
        .getSubject(subjectType.name)
      subjectNode
        .getOrCreate(() => subjectType)
        .flatMap(
          o =>
            guaranteeTopic(o)
              .map(
                _ =>
                  new KafkaGenericSink(subjectNode,
                                       jobNode,
                                       kafkaProducerFactory,
                                       epochStateManager,
                                       sinkId
                                       //subjectFactory.getTransformer[TData](subjectType)
                )))
    }

    /**
      * Get a generic sink for the given type
      *
      * @param subjectType subject to obtain the sink for
      * @return
      */
    def getSink(subjectType: SubjectType,
                jobName: String,
                sinkId: String): Future[SinkFunction[TrailedRecord]] =
      async {
        val subjectNode = subjectLibrary.getSubject(subjectType.name)
        val jobNode = subjectLibrary.getJob(jobName)
        await(guaranteeTopic(subjectType))
        new TrailedRecordSink(subjectNode,
                              jobNode,
                              kafkaProducerFactory,
                              epochStateManager,
                              sinkId)
      }

    /**
      * Return a sink for the tableApi
      *
      * @param subjectType subject to obtain rowsink for
      * @return
      */
    def getRowSink(subjectType: SubjectType, jobName: String, sinkId: String): RowSink = {
      val subjectNode = subjectLibrary.getSubject(subjectType.name)
      val jobNode = subjectLibrary.getJob(jobName)
      guaranteeTopicBlocking(subjectType)
      new RowSink(subjectNode, jobNode, kafkaProducerFactory, epochStateManager, sinkId)
    }

    /**
      * Construct a serializable and distributable mapper function from any source type to a TrailedRecord
      *
      * @tparam TData Type of the source object to get a mapper for
      * @return A function that can convert the object into a trailed record
      */
    def getTransformer[TData: ru.TypeTag: ClassTag](
        subjectType: SubjectType): TData => TrailedRecord = {
      @transient lazy val transformer = new RecordTransformer[TData](subjectType)
      @transient lazy val keyFactory = new KeyFactory(subjectType, UUID.randomUUID())
      d: TData =>
        {
          val record = transformer.bag(d, ActionType.Add)
          val trail = keyFactory.getKey(record)
          TrailedRecord(record, trail)
        }
    }

    /**
      * Construct a deserializer to transform a TrailedRecord back into some object
      * Meant to use for development & testing, not very safe
      *
      * @param subjectType The type of the record expected
      * @tparam TData Type of the object to transform to
      * @return The object
      */
    def getUnTransformer[TData: ru.TypeTag: ClassTag](
        subjectType: SubjectType): TrailedRecord => TData = { r: TrailedRecord =>
      {
        val transformer = new RecordTransformer[TData](subjectType)
        transformer.unbag(r)
      }
    }

    def getRowSource(subjectNode: SubjectNode,
                     jobNode: JobNode,
                     sourceId: String): SourceFunction[Row] = {
      new KafkaRowSource(subjectNode, jobNode, kafkaConsumerFactory, sourceId)
    }

    def getSource(subjectNode: SubjectNode,
                  jobNode: JobNode,
                  sinkId: String): SourceFunction[TrailedRecord] = {
      new KafkaTrailedRecordSource(subjectNode, jobNode, kafkaConsumerFactory, sinkId)
    }

    /***
      * Guarantees a topic for the given subject is created on Kafka
      */
    private def guaranteeTopic(st: SubjectType): Future[Unit] = {
      KafkaController
        .guaranteeTopic(s"${st.name}_${st.uuid}",
                        conf.getInt("codefeedr.kafka.custom.partition.count"))
    }

    /**
      * Waits blocking for the kafkaTopic to be created
      * @param st subject to await topic for
      */
    private def guaranteeTopicBlocking(st: SubjectType): Unit = {
      Await.ready(guaranteeTopic(st), Duration(5, SECONDS))
    }
  }

}
