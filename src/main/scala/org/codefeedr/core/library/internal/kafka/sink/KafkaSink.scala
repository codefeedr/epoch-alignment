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
import java.util.{Optional, UUID}

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.FunctionSnapshotContext
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction, TwoPhaseCommitSinkFunction}
import org.apache.flink.types.Row
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.codefeedr.core.library.internal.KeyFactory
import org.codefeedr.core.library.LibraryServices
import org.codefeedr.core.library.metastore.{ProducerNode, QuerySinkNode, SubjectNode}
import org.codefeedr.model.zookeeper.{Producer, QuerySink}
import org.codefeedr.model.{RecordSourceTrail, _}

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
 abstract class KafkaSink[TSink]()
    extends TwoPhaseCommitSinkFunction[TSink, TransactionState, TransactionContext](transactionStateSerializer, transactionContextSerializer)
    with LazyLogging
    with Serializable
    with LibraryServices {


  protected val subjectType: SubjectType
  protected val sinkUuid: String

  @transient protected lazy val subjectNode: SubjectNode = subjectLibrary.getSubject(subjectType.name)

  @transient protected lazy val sinkNode: QuerySinkNode = subjectNode.getSinks().getChild(sinkUuid)

  /**
    * The ZookeeperNode that represent this instance of the producer
    */
  @transient protected lazy val producerNode: ProducerNode = sinkNode.getProducers().getChild(instanceUuid)

  @transient protected lazy val topic = s"${subjectType.name}_${subjectType.uuid}"
  //A random identifier for this specific sink
  @transient protected lazy  val instanceUuid: String = UUID.randomUUID().toString

  //Size (amount) of kafkaProducers
  @transient private lazy val producerPoolSize: Long = ConfigFactory.load.getLong("codefeedr.kafka.custom.producer.count")

  //Current set of available kafka producers
  @transient protected lazy val producerPool: List[KafkaProducer[RecordSourceTrail, Row]] =
    (1 to producerPoolSize).map(_ => KafkaProducerFactory.create[RecordSourceTrail, Row]).toList


  def getLabel(): String = s"KafkaSink ${subjectType.name}($sinkUuid-$instanceUuid)"


  override def initializeUserContext(): Optional[TransactionContext] =
    Optional.of(TransactionContext((0 until producerPoolSize).map(id => id -> true).toMap))


  override def close(): Unit = {
    if(getUserContext.get().availableProducers.exists(o => o._2)) {
      throw new Exception(s"Error while closing producer ${getLabel()}. There are still uncommitted transactions")
    }
    Await.ready(producerNode.setState(false), Duration.Inf)
    logger.debug(s"Closing producer ${getLabel()}")
  }


  override def open(parameters: Configuration): Unit = {
    logger.debug(s"Opening producer ${getLabel()} for ${subjectType.name}")
    //Create zookeeper nodes synchronous
    Await.ready(subjectNode.create(subjectType), Duration(5, SECONDS))
    Await.ready(sinkNode.create(QuerySink(sinkUuid)), Duration(5, SECONDS))
    Await.ready(producerNode.create(Producer(instanceUuid, null, System.currentTimeMillis())),Duration(5, SECONDS))
    Await.ready(producerNode.setState(true), Duration.Inf)
    logger.debug(s"Producer ${getLabel()} created for topic $topic")
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    super.snapshotState(context)
    //Save the checkpointId on the transaction, so it can be tracked to the right epoch
    currentTransaction().checkPointId = context.getCheckpointId
    val producerIndex = context.getCheckpointId % producerPoolSize
    if(!getUserContext.get().availableProducers(producerIndex)) {
      throw new Exception(s"Error while obtaining a new producer for ${getLabel()}. Requested producer has not yet performed its commit. Either increase producer pool, reduce checkpoint interval or make checkpoints run faster")
    }
    currentTransaction().producer = producerPool(producerIndex.toInt)
    //Set the producer to be unavailable
    getUserContext.get().availableProducers(producerIndex) = false
    logger.debug(s"${getLabel()} started transaction ${context.getCheckpointId} on producer $producerIndex")
  }


  /**
    * Transform the sink type into the type that is actually sent to kafka
    * @param value
    * @return
    */
  def transform(value: TSink): (RecordSourceTrail, Row)

  override def invoke(transaction: TransactionState, value: TSink, context: SinkFunction.Context[_]): Unit = {
    val event = transform(value)
    val record = new ProducerRecord[RecordSourceTrail, Row](topic,event._1, event._2)
    //TODO: Obtain offsets from the future
    val metaF = transaction.producer.send(record)
  }

  override def commit(transaction: TransactionState): Unit = {
    transaction.producer.commitTransaction()
  }

  /**
    * Perform all pre-commit operations. Commit is not allowed to fail after precommit
    * @param transaction
    */
  override def preCommit(transaction: TransactionState): Unit = {
    transaction.producer.flush()
  }

  override def beginTransaction(): TransactionState = new TransactionState()

  override def abort(transaction: TransactionState): Unit = {
    transaction.producer.abortTransaction()
    getUserContext.get().availableProducers(transaction.checkPointId % producerPoolSize) = true
  }



}

