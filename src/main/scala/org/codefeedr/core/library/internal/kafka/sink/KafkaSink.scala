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
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.functions.sink.{
  RichSinkFunction,
  SinkFunction,
  TwoPhaseCommitSinkFunction
}
import org.apache.flink.types.Row
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.codefeedr.core.library.internal.KeyFactory
import org.codefeedr.core.library.LibraryServices
import org.codefeedr.core.library.metastore.{ProducerNode, QuerySinkNode, SubjectNode}
import org.codefeedr.model.zookeeper.{Producer, QuerySink}
import org.codefeedr.model.{RecordSourceTrail, _}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise, blocking}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

/**
  * A simple kafka sink, pushing all records to a kafka topic of the given subjecttype
  * Not thread safe
  * Because this class needs to be serializable and the LibraryServices are not, no dependency injection structure can be used here :(
  * Serializable (with lazy initialisation)
  * Created by Niels on 11/07/2017.
  */
abstract class KafkaSink[TSink]()
    extends TwoPhaseCommitSinkFunction[TSink, TransactionState, TransactionContext](
      transactionStateSerializer,
      transactionContextSerializer)
    with LazyLogging
    with Serializable
    with LibraryServices {

  protected var subjectType: SubjectType
  protected val sinkUuid: String

  @transient protected lazy val subjectNode: SubjectNode =
    subjectLibrary.getSubject(subjectType.name)

  @transient protected lazy val sinkNode: QuerySinkNode = subjectNode.getSinks().getChild(sinkUuid)

  /**
    * The ZookeeperNode that represent this instance of the producer
    */
  @transient protected lazy val producerNode: ProducerNode =
    sinkNode.getProducers().getChild(instanceUuid)

  @transient protected lazy val topic = s"${subjectType.name}_${subjectType.uuid}"
  //A random identifier for this specific sink
  @transient protected lazy val instanceUuid: String = UUID.randomUUID().toString

  //Size (amount) of kafkaProducers
  @transient private lazy val producerPoolSize: Int =
    ConfigFactory.load.getInt("codefeedr.kafka.custom.producer.count")

  //Current set of available kafka producers
  @transient protected lazy val producerPool: List[KafkaProducer[RecordSourceTrail, Row]] =
    (1 to producerPoolSize)
      .map(i => KafkaProducerFactory.create[RecordSourceTrail, Row](s"$instanceUuid->$i"))
      .toList

  /**
    * Transform the sink type into the type that is actually sent to kafka
    * @param value
    * @return
    */
  def transform(value: TSink): (RecordSourceTrail, Row)

  def getLabel(): String = s"KafkaSink ${subjectType.name}($sinkUuid-$instanceUuid)"

  def getFirstFreeProducerIndex() =
    getUserContext.get().availableProducers.zipWithIndex.filter(_._1._2).values.head

  override def initializeUserContext(): Optional[TransactionContext] =
    Optional.of(
      TransactionContext(mutable.Map() ++ (0 until producerPoolSize).map(id => id -> true)))

  override def close(): Unit = {

    logger.debug(s"Closing producer ${getLabel()}")

    //HACK: Comitting current transaction should not happen here!!!
    //This is a must to support the current sources, this should be removed ASAP
    if (currentTransaction() != null) {
      //HACK: Need to ensure all producers are created before the first is closed
      //Probably automatically fixed when we no longer auto-commit upon closing
      blocking {
        Thread.sleep(200)
      }
      logger.debug(s"Committing current transaction")
      commit(currentTransaction())
    }
    //HACK: When ran from unit test this context is null
    //Somehow need to mock this away
    if (getUserContext != null) {
      if (getUserContext.get().availableProducers.exists(o => !o._2)) {
        logger.error(
          s"Error while closing producer ${getLabel()}. There are still uncommitted transactions")
        //throw new Exception()
      }
    }
    Await.ready(producerNode.setState(false), Duration.Inf)
  }

  override def open(parameters: Configuration): Unit = {
    logger.debug(s"Opening producer ${getLabel()} for ${subjectType.name}")
    //Create zookeeper nodes synchronous
    //TODO: Validate created subject type actually matches the expected type.
    subjectType = Await.result(subjectNode.getOrCreate(() => subjectType), Duration(5, SECONDS))
    Await.ready(sinkNode.create(QuerySink(sinkUuid)), Duration(5, SECONDS))
    Await.ready(producerNode.create(Producer(instanceUuid, null, System.currentTimeMillis())),
                Duration(5, SECONDS))
    Await.ready(producerNode.setState(true), Duration.Inf)
    logger.debug(s"Producer ${getLabel()} created for topic $topic")
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    super.snapshotState(context)
    logger.debug(s"snapshot State called on ${getLabel()}")
    //Save the checkpointId on the transaction, so it can be tracked to the right epoch
    currentTransaction().checkPointId = context.getCheckpointId
    logger.debug(
      s"${getLabel()} assigned transaction on producer ${currentTransaction().producerIndex} to checkpoint ${context.getCheckpointId}")
  }

  override def invoke(transaction: TransactionState,
                      value: TSink,
                      context: SinkFunction.Context[_]): Unit = {
    logger.debug(
      s"${getLabel()} sending event on transaction ${transaction.checkPointId} producer ${transaction.producerIndex}")
    val event = transform(value)
    val record = new ProducerRecord[RecordSourceTrail, Row](topic, event._1, event._2)

    //Wrap the callback into a proper scala future
    val p = Promise[RecordMetadata]
    //Notify state an event has been sent
    transaction.sent()
    producerPool(transaction.producerIndex).send(
      record,
      new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception == null) {
            p.success(metadata)
          } else {
            p.failure(exception)
          }
        }
      }
    )

    //For now just throw any exception. Flink should catch this and restore the worker
    p.future.onComplete {
      case Success(rm) => transaction.confirmed(rm)
      case Failure(e) => throw e
    }
  }

  /**
    * Await result from all pending events, and perform the actual commit
    * @param transaction
    */
  override def commit(transaction: TransactionState): Unit = {
    logger.debug(
      s"${getLabel()} committing transaction ${transaction.checkPointId}.\r\n${transaction.displayOffsets()}")
    producerPool(transaction.producerIndex).commitTransaction()
    getUserContext.get().availableProducers(transaction.producerIndex) = true
  }

  /**
    * Perform all pre-commit operations. Commit is not allowed to fail after precommit
    * Blocking operation
    * @param transaction
    */
  override def preCommit(transaction: TransactionState): Unit = blocking {
    producerPool(transaction.producerIndex).flush()
    logger.debug(s"${getLabel()} flushed and is awaiting events to commit")
    Await.ready(transaction.awaitCommit(), Duration(5, SECONDS))
  }

  override def beginTransaction(): TransactionState = {
    val producerIndex = getFirstFreeProducerIndex()
    getUserContext.get().availableProducers(producerIndex) = false
    logger.debug(s"${getLabel()} started new transaction on producer ${producerIndex}")
    producerPool(producerIndex).beginTransaction()
    new TransactionState(producerIndex)
  }

  override def abort(transaction: TransactionState): Unit = {
    logger.debug(
      s"${getLabel()} aborting transaction ${transaction.checkPointId} on producer ${transaction.producerIndex}")
    producerPool(transaction.producerIndex).abortTransaction()
    getUserContext
      .get()
      .availableProducers(transaction.producerIndex) = true
  }

}
