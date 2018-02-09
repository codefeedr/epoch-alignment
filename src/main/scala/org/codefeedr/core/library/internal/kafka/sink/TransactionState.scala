package org.codefeedr.core.library.internal.kafka.sink

import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton
import org.apache.flink.api.common.typeutils.{CompatibilityResult, TypeSerializer, TypeSerializerConfigSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.types.Row
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.codefeedr.model.RecordSourceTrail

/**
  * Mutable class maintaining the state of a running transaction to Kafka
  * TODO: Expand this with the size of the pool to support reconfiguration after a restore
  * @param checkPointId Identification of the checkpoint that belongs to the transaction
  * @param producer KafkaProducer used for the transaction
  */
class TransactionState(var checkPointId: Long = 0, var producer: KafkaProducer[RecordSourceTrail, Row] = null)
{
}


object transactionStateSerializer extends TypeSerializerSingleton[TransactionState] {
  override def createInstance(): TransactionState = new TransactionState()

  override def canEqual(obj: scala.Any): Boolean = obj == this

  override def serialize(record: TransactionState, target: DataOutputView): Unit = {
    target.writeLong(record.checkPointId)
  }

  override def isImmutableType: Boolean = false

  override def getLength: Int = -1

  override def copy(from: TransactionState): TransactionState = copy(from, new TransactionState())

  override def copy(from: TransactionState, reuse: TransactionState): TransactionState = {
    reuse.checkPointId = from.checkPointId
    reuse.producer = null
    reuse
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    target.writeLong(source.readLong())
  }

  /**
    * Deserialize to a new state
    * @param source
    * @return
    */
  override def deserialize(source: DataInputView): TransactionState = deserialize(new TransactionState(), source)

  /**
    * TODO: Actually suport reuse
    * @param reuse
    * @param source
    * @return
    */
  override def deserialize(reuse: TransactionState, source: DataInputView): TransactionState = {
    reuse.checkPointId = source.readLong()
    reuse.producer = null
    reuse
  }
}