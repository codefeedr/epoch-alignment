package org.codefeedr.core.library.internal.kafka.sink

import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable

case class TransactionContext(availableProducers: mutable.Map[Int, Boolean])

object transactionContextSerializer extends TypeSerializerSingleton[TransactionContext] {
  override def createInstance(): TransactionContext = null

  override def canEqual(obj: scala.Any): Boolean = obj == this

  override def serialize(record: TransactionContext, target: DataOutputView): Unit = {
    target.writeInt(record.availableProducers.size)
    record.availableProducers.foreach(o => {
      target.writeInt(o._1)
      target.writeBoolean(o._2)
    })
  }

  //The type is immutable, but some if its properties are not
  //However, objects should not be reused
  override def isImmutableType: Boolean = true

  override def getLength: Int = -1

  override def copy(from: TransactionContext): TransactionContext =
    TransactionContext(from.availableProducers.clone().asInstanceOf[mutable.Map[Int, Boolean]])

  override def copy(from: TransactionContext, reuse: TransactionContext): TransactionContext =
    copy(from)

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    val length = source.readInt()
    val availableProducers = (1 to length).foreach(_ => {
      target.writeInt(source.readInt())
      target.writeBoolean(source.readBoolean())
    })
  }

  override def deserialize(source: DataInputView): TransactionContext = {
    val length = source.readInt()
    val availableProducers = (1 to length).map(_ => {
      val id = source.readInt()
      val available = source.readBoolean()
      id -> available
    })
    TransactionContext(mutable.Map() ++ availableProducers)
  }

  override def deserialize(reuse: TransactionContext, source: DataInputView): TransactionContext =
    deserialize(source)
}
