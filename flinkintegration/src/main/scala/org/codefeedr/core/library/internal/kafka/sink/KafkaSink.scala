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
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.{
  RichSinkFunction,
  SinkFunction,
  TwoPhaseCommitSinkFunction
}
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.apache.flink.types.Row
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.codefeedr.core.library.internal.KeyFactory
import org.codefeedr.core.library.LibraryServices
import org.codefeedr.core.library.metastore.{JobNode, ProducerNode, QuerySinkNode, SubjectNode}
import org.codefeedr.model.zookeeper.{Producer, QuerySink}
import org.codefeedr.model.{RecordSourceTrail, _}
import org.codefeedr.util.Stopwatch

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
abstract class KafkaSink[TSink](subjectNode: SubjectNode,
                                jobNode: JobNode,
                                kafkaProducerFactory: KafkaProducerFactory,
                                epochStateManager: EpochStateManager)
    extends TwoPhaseCommitSinkFunction[TSink, TransactionState, TransactionContext](
      transactionStateSerializer,
      transactionContextSerializer)
    with LazyLogging
    with Serializable {

  protected val sinkUuid: String

  @transient private[kafka] var checkpointingMode: Option[CheckpointingMode] = _

  protected var opened: Boolean = false

  @transient protected lazy val subjectType: SubjectType =
    subjectNode.getDataSync().get

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
  @transient protected[sink] lazy val producerPool: List[KafkaProducer[RecordSourceTrail, Row]] =
    (1 to producerPoolSize)
      .map(i => kafkaProducerFactory.create[RecordSourceTrail, Row](s"${instanceUuid}_$i"))
      .toList

  /** The index of this parallel subtask */
  @transient private lazy val parallelIndex = getRuntimeContext.getIndexOfThisSubtask

  /**
    * Transform the sink type into the type that is actually sent to kafka
    * @param value
    * @return
    */
  def transform(value: TSink): (RecordSourceTrail, Row)

  def getLabel(): String = s"KafkaSink ${subjectNode.name}($sinkUuid-$instanceUuid)"

  def getFirstFreeProducerIndex() =
    getUserContext.get().availableProducers.filter(o => o._2).keys.head

  override def initializeUserContext(): Optional[TransactionContext] =
    Optional.of(
      TransactionContext(mutable.Map() ++ (0 until producerPoolSize).map(id => id -> true)))

  override def close(): Unit = {
    logger.debug(s"Closing producer ${getLabel()}")
    Await.ready(producerNode
                  .setState(false)
                  .andThen({
                    case _ => logger.debug(s"Closed producer ${getLabel()}")
                  }),
                5.seconds)
  }

  override def open(parameters: Configuration): Unit = {
    if (opened) {
      throw new Exception(s"Open on sink called twice: ${getLabel()}")
    }

    getRuntimeContext().getNumberOfParallelSubtasks

    if (!getRuntimeContext().asInstanceOf[StreamingRuntimeContext].isCheckpointingEnabled) {
      logger.warn(
        "Started a custom sink without checkpointing enabled. The custom source is designed to work with checkpoints only.")
      checkpointingMode = None
    } else {
      checkpointingMode = Some(CheckpointingMode.EXACTLY_ONCE)
    }

    //Temporary check if opened
    opened = true
    logger.debug(s"Opening producer ${getLabel()} for ${subjectType.name}")
    //Create zookeeper nodes synchronous
    if (!Await.result(subjectNode.exists(), 5.seconds)) {
      throw new Exception(s"Cannot open source ${getLabel()} because its subject does not exist.")
    }
    Await.ready(sinkNode.create(QuerySink(sinkUuid)), 5.seconds)
    Await.ready(producerNode.create(Producer(instanceUuid, null, System.currentTimeMillis())),
                5.seconds)
    Await.ready(producerNode.setState(true), 5.seconds)
    logger.debug(s"Producer ${getLabel()} created for topic $topic")
  }
  //Used for test
  override protected[sink] def currentTransaction() = super.currentTransaction()

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    logger.debug(
      s"snapshot State called on ${getLabel()} with checkpoint ${context.getCheckpointId}")
    //Save the checkpointId on the transaction, so it can be tracked to the right epoch
    //Perform this operation before calling snapshotState on the parent, because it will start a new transaction
    currentTransaction().checkPointId = context.getCheckpointId
    logger.debug(
      s"${getLabel()} assigned transaction on producer ${currentTransaction().producerIndex} to checkpoint ${context.getCheckpointId}")
    super.snapshotState(context)
  }

  override def invoke(transaction: TransactionState,
                      value: TSink,
                      context: SinkFunction.Context[_]): Unit = {
    logger.debug(
      s"${getLabel()} sending event on transaction ${transaction.checkPointId} producer ${transaction.producerIndex}")
    val event = transform(value)
    val record =
      new ProducerRecord[RecordSourceTrail, Row](topic, parallelIndex, event._1, event._2)

    //Wrap the callback into a proper scala promise
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
    * TODO: Validate this does not block processing new events
    * Otherwise we might just perform this task on the background. We are not allowed to throw exceptions from the commit method anyway
    * @param transaction
    */
  override def commit(transaction: TransactionState): Unit = {
    val sw = Stopwatch.start()
    logger.debug(
      s"${getLabel()} committing transaction ${transaction.checkPointId} on producer ${transaction.producerIndex}.\r\n${transaction
        .displayOffsets()}\r\n${getLabel()}")
    producerPool(transaction.producerIndex).commitTransaction()
    val epochState = EpochState(transaction, subjectNode.getEpochs())
    Await.ready(epochStateManager.commit(epochState), 5.seconds)
    getUserContext.get().availableProducers(transaction.producerIndex) = true
    logger.debug(
      s"${getLabel()} done committing transaction ${transaction.checkPointId}.\r\n${transaction.displayOffsets()}")
    logger.debug(
      s"Commit completed in ${sw.elapsed().toMillis} transaction ${transaction.checkPointId}.\r\n${transaction
        .displayOffsets()}\r\n${getLabel()}")
  }

  /**
    * Perform all pre-commit operations. Commit is not allowed to fail after precommit
    * Blocking operation
    * @param transaction
    */
  override def preCommit(transaction: TransactionState): Unit = {
    val sw = Stopwatch.start()
    logger.debug(
      s"${getLabel()} Precomitting transaction ${transaction.checkPointId} on producer ${transaction.producerIndex}.\r\n${transaction
        .displayOffsets()}\r\n${getLabel()}")
    blocking {
      //TODO: Validate if this should/can be replaced by just a flush
      Await.ready(transaction.awaitCommit(), 5.seconds)
      //producerPool(transaction.producerIndex).flush()
      logger.debug(
        s"flushed and is awaiting events to commit in ${sw.elapsed().toMillis}\r\n${getLabel()}")
      //Perform precommit on the epochState
      val epochState = EpochState(transaction, subjectNode.getEpochs())
      Await.ready(epochStateManager.preCommit(epochState), 5.seconds)
    }
    logger.debug(
      s"Precommit completed in ${sw.elapsed().toMillis} transaction ${transaction.checkPointId}.\r\n${transaction
        .displayOffsets()}\r\n${getLabel()}")
  }

  override def beginTransaction(): TransactionState = {
    logger.debug(s"beginTransaction called on ${getLabel()}. Opened: $opened")
    val producerIndex = getFirstFreeProducerIndex()
    getUserContext.get().availableProducers(producerIndex) = false
    producerPool(producerIndex).beginTransaction()
    logger.debug(s"${getLabel()} started new transaction on producer ${producerIndex}")

    val nt = new TransactionState(producerIndex)
    val ct = currentTransaction()
    //Update new transaction with the offsets of the previous transaction
    if (ct != null) {
      nt.offsetMap = ct.offsetMap.clone()
    }
    nt
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
