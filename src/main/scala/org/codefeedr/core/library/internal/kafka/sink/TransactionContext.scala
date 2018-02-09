package org.codefeedr.core.library.internal.kafka.sink

import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.kafka.common.TopicPartition

case class TransactionContext(availableProducers: Map[Long, Boolean])

object transactionContextSerializer extends TypeSerializerSingleton[TransactionContext] {
  override def createInstance(): TransactionContext = null

  override def canEqual(obj: scala.Any): Boolean = obj == this

  override def serialize(record: TransactionContext, target: DataOutputView): Unit = {
    target.writeInt(record.availableProducers.size)
    record.availableProducers.foreach(o => {
      target.writeLong(o._1)
      target.writeBoolean(o._2)
    })
  }

  override def isImmutableType: Boolean = true

  override def getLength: Int = -1

  override def copy(from: TransactionContext): TransactionContext =
    TransactionContext(from.availableProducers.clone().asInstanceOf[Map[Long, Boolean]])

  override def copy(from: TransactionContext, reuse: TransactionContext): TransactionContext = copy(from)

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    val length = source.readInt()
    val availableProducers = (1 to length).foreach(_ => {
      target.writeLong(source.readLong())
      target.writeBoolean(source.readBoolean())
    })
  }

  override def deserialize(source: DataInputView): TransactionContext = {
    val length = source.readInt()
    val availableProducers = (1 to length).map(_ => {
      val id = source.readLong()
      val available = source.readBoolean()
      id -> available
    }).toMap
    TransactionContext(availableProducers)
  }

  override def deserialize(reuse: TransactionContext, source: DataInputView): TransactionContext = deserialize(source)
}
