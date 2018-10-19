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
import org.codefeedr.configuration.KafkaConfigurationComponent
import org.codefeedr.core.library.internal.kafka._
import org.codefeedr.core.library.internal.kafka.sink._
import org.codefeedr.core.library.internal.kafka.source.{
  KafkaConsumerFactoryComponent,
  KafkaGenericSource,
  KafkaGenericTrailedSource,
  KafkaRowSource
}
import org.codefeedr.core.library.internal.{KeyFactory, RecordTransformer, SubjectTypeFactory}
import org.codefeedr.core.library.metastore.{JobNode, SubjectLibraryComponent, SubjectNode}
import org.codefeedr.model.zookeeper.QuerySource
import org.codefeedr.model.{ActionType, SubjectType, TrailedRecord}
import org.codefeedr.util.EventTime

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

trait SubjectFactoryComponent extends Serializable {
  this: SubjectLibraryComponent
    with KafkaProducerFactoryComponent
    with KafkaConsumerFactoryComponent
    with KafkaControllerComponent
    with EpochStateManagerComponent
    with KafkaConfigurationComponent =>

  private val subjectFactoryComponent = this
  private val timeout = Duration(5, SECONDS)

  val subjectFactory: SubjectFactoryController

  /**
    * ThreadSafe
    * Created by Niels on 18/07/2017.
    */
  class SubjectFactoryController {

    /**
      * Retrieve a basic sink of the given generic type
      * @param sinkId unique id of the sink
      * @param jobName name of the job the sink belongs to
      * @tparam TData generic type of the element in the job
      * @return
      */
    def getSink[TData: ClassTag: EventTime](sinkId: String,
                                            jobName: String): Future[SinkFunction[TData]] =
      async {
        val (subjectNode, jobNode) = getSubjectJobNode[TData](jobName)
        await(validateSubject(subjectNode))
        new KafkaGenericSink(subjectNode,
                             jobNode,
                             kafkaConfiguration,
                             kafkaProducerFactory,
                             epochStateManager,
                             sinkId)
      }

    def getSource[TData: ClassTag: ru.TypeTag: EventTime](
        sinkId: String,
        jobName: String): Future[SourceFunction[TData]] = async {
      val (subjectNode, jobNode) = getSubjectJobNode[TData](jobName)
      await(validateSubject(subjectNode))
      getSource[TData](subjectNode, jobNode, sinkId)
    }

    /**
      * Retrieve a trailed record sink for the given generic type
      * @param sinkId
      * @param jobName
      * @tparam TData
      * @return
      */
    def getGenericTrailedSink[TData: ClassTag: EventTime](
        sinkId: String,
        jobName: String): Future[GenericTrailedRecordSink[TData]] = async {
      val (subjectNode, jobNode) = getSubjectJobNode[TData](jobName)
      await(validateSubject(subjectNode))
      new GenericTrailedRecordSink[TData](subjectNode,
                                          jobNode,
                                          kafkaConfiguration,
                                          kafkaProducerFactory,
                                          epochStateManager,
                                          sinkId)(subjectFactoryComponent)
    }

    def getTrailedSink(subjectType: SubjectType,
                       sinkId: String,
                       jobName: String): TrailedRecordSink = {
      val (subjectNode, jobNode) = getSubjectJobNode(subjectType.name, jobName)
      new TrailedRecordSink(subjectNode,
                            jobNode,
                            kafkaConfiguration,
                            kafkaProducerFactory,
                            epochStateManager,
                            sinkId)
    }

    /**
      * retrieves the subjectnode and jobnode
      * @param jobName name of the job sink belongs to
      * @tparam TData entity type of the sink
      * @return
      */
    private def getSubjectJobNode[TData: ClassTag](jobName: String): (SubjectNode, JobNode) = {
      val subjectName = SubjectTypeFactory.getSubjectName[TData]
      getSubjectJobNode(subjectName, jobName)
    }

    private def getSubjectJobNode(subjectName: String, jobName: String): (SubjectNode, JobNode) = {
      val jobNode = subjectLibrary.getJob(jobName)
      val subjectNode = subjectLibrary
        .getSubject(subjectName)
      (subjectNode, jobNode)
    }

    /**
      * Validates if a subject is properly registered
      * @return
      */
    private def validateSubject(subjectNode: SubjectNode): Future[Unit] = async {
      await(
        subjectNode
          .getData()
          .map {
            case Some(v) => v
            case None =>
              throw new IllegalStateException(
                s"Subject ${subjectNode.name} has not been registered yet. Forgot to register it?")
          })
    }

    /**
      * Get a generic sink for the given type
      *

      * @return
      */
    /*
    def getSink(subjectType: SubjectType,
                jobName: String,
                sinkId: String): Future[SinkFunction[TrailedRecord]] =
      async {
        val subjectNode = subjectLibrary.getSubject(subjectType.name)
        await(subjectNode.assertExists())
        val jobNode = subjectLibrary.getJob(jobName)
        new TrailedRecordSink(subjectNode,
                              jobNode,
                              kafkaProducerFactory,
                              epochStateManager,
                              sinkId)
      }*/

    /**
      * Return a sink for the tableApi
      *
      * @param subjectType subject to obtain rowsink for
      * @return
      */
    def getRowSink(subjectType: SubjectType, jobName: String, sinkId: String): RowSink = {
      val subjectNode = subjectLibrary.getSubject(subjectType.name)
      val jobNode = subjectLibrary.getJob(jobName)
      new RowSink(subjectNode,
                  jobNode,
                  kafkaConfiguration,
                  kafkaProducerFactory,
                  epochStateManager,
                  sinkId)
    }

    /**
      * Construct a serializable and distributable mapper function from any source type to a TrailedRecord
      *
      * @tparam TData Type of the source object to get a mapper for
      * @return A function that can convert the object into a trailed record
      */
    def getTransformer[TData: ClassTag](subjectType: SubjectType): TData => TrailedRecord = {
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

    private def createSourceNode(subjectNode: SubjectNode, sourceId: String): Unit = {
      val sourceNode = subjectNode.getSources().getChild(sourceId)
      Await.result(sourceNode.create(QuerySource(sourceId)), timeout)
    }

    def getRowSource(subjectNode: SubjectNode,
                     jobNode: JobNode,
                     sourceId: String): SourceFunction[Row] = {
      createSourceNode(subjectNode, sourceId)
      new KafkaRowSource(subjectNode, jobNode, kafkaConfiguration, kafkaConsumerFactory, sourceId)
    }

    def getSource[TSource: ClassTag: ru.TypeTag: EventTime](subjectNode: SubjectNode,
                                                            jobNode: JobNode,
                                                            sourceId: String) = {
      createSourceNode(subjectNode, sourceId)
      new KafkaGenericSource[TSource](subjectNode,
                                      jobNode,
                                      kafkaConfiguration,
                                      kafkaConsumerFactory,
                                      sourceId)
    }

    def getTrailedSource(subjectNode: SubjectNode,
                         jobNode: JobNode,
                         sourceId: String): SourceFunction[TrailedRecord] = {
      createSourceNode(subjectNode, sourceId)
      new KafkaTrailedRecordSource(subjectNode,
                                   jobNode,
                                   kafkaConfiguration,
                                   kafkaConsumerFactory,
                                   sourceId)
    }

    /**
      * Creates a subject. Both the zookeeper subject and kafka topic are created.
      * Once the zookeeper subjectNode exists, one can assume the kafka topic also exists
      * @tparam TSubject Type of the subject to create
      * @return the subject node of the created subject
      */
    def create[TSubject: ClassTag: ru.TypeTag](): Future[SubjectNode] = {
      val subjectType = SubjectTypeFactory.getSubjectType[TSubject]
      create(subjectType)
    }

    /**
      * Removes the given type from the zookeeper registration, and then adds it again
      * Make sure to only use this method when you are sure nothing is still using this subject
      * This method is meant for development purposes
      * Otherwise you might end up with unexpected behavior
      * @tparam TSubject the subject to create
      * @return the created subjectNode
      */
    def reCreate[TSubject: ClassTag: ru.TypeTag](): Future[SubjectNode] = async {
      val subjectType = SubjectTypeFactory.getSubjectType[TSubject]
      await(subjectLibrary.getSubject(subjectType.name).deleteRecursive())
      await(create(subjectType))
    }

    /**
      * Creates a subject. Both the zookeeper subject and kafka topic are created.
      * Once the zookeeper subjectNode exists, one can assume the kafka topic also exists
      * @param subjectType Type of the subject to create
      * @return the subject node of the created subject
      */
    def create(subjectType: SubjectType): Future[SubjectNode] = async {
      //Make sure to first create the kafka topic, and only create the subject after!
      await(guaranteeTopic(subjectType))
      val subjectNode = subjectLibrary.getSubject(subjectType.name)
      //Next create the subjectNode in zookeeper
      await(subjectNode.create(subjectType))
      subjectNode
    }

    /***
      * Guarantees a topic for the given subject is created on Kafka
      */
    private def guaranteeTopic(st: SubjectType): Future[Unit] = {
      kafkaController
        .guaranteeTopic(kafkaConfiguration.getTopic(st))
    }

    /**
      * Waits blocking for the kafkaTopic to be created
      * @param st subject to await topic for
      */
    private def guaranteeTopicBlocking(st: SubjectType): Unit = {
      Await.ready(guaranteeTopic(st), timeout)
    }
  }

}
