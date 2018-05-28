package org.codefeedr.core.library.internal.kafka.sink

import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton
import org.apache.flink.api.common.typeutils.{
  CompatibilityResult,
  TypeSerializer,
  TypeSerializerConfigSnapshot
}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.types.Row
import org.apache.kafka.clients.producer.{KafkaProducer, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.codefeedr.model.RecordSourceTrail

import scala.collection.mutable
import scala.concurrent.{Future, Promise}

/**
  * Mutable class maintaining the state of a running transaction to Kafka
  * TODO: Expand this with the size of the pool to support reconfiguration after a restore
  * TODO: Use immutable class (but Flink's API was mainly built for mutable scala classes)
  * TODO: Unit test this class!
  * @param producerIndex index of the used producer
  * @param checkPointId Identification of the checkpoint that belongs to the transaction
  * @param pendingEvents Counter observing pending events
  * @param awaitingCommit Boolean indicating if the transaction is awaiting events to perform a commit
  * @param offsetMap latest offsets per topicpartition for the transaction
  */
class TransactionState(
    var producerIndex: Int = 0,
    var checkPointId: Long = 0,
    @volatile var pendingEvents: Int = 0,
    @volatile var awaitingCommit: Boolean = false,
    var offsetMap: mutable.Map[Int, Long] = mutable.Map[Int, Long]()
) {
  //Promise that is true when a commit can be done
  @transient private lazy val awaitCommitPromise = Promise[Unit]

  /**
    * Notify the state that an event has been sent to kafka
    */
  def sent(): Unit = synchronized {
    if (awaitingCommit) {
      throw new Exception(
        "Cannot send another event to kafka while transaction is already awaiting a commit.")
    }
    pendingEvents += 1
  }

  /**
    * Notify the state an event has been recieved by kafka
    * Used to save offsets in the
    */
  def confirmed(recordMetadata: RecordMetadata): Unit = synchronized {
    val tp = recordMetadata.partition()

    //Only update the offset if the new offset is new or higher
    offsetMap.get(tp) match {
      case None => offsetMap.put(tp, recordMetadata.offset())
      case Some(v) => if (v < recordMetadata.offset()) { offsetMap(tp) = recordMetadata.offset() }
    }

    this.synchronized {
      pendingEvents -= 1
      if (awaitingCommit && pendingEvents == 0) {
        awaitCommitPromise.success()
      }
    }
  }

  /**
    * Returns a promise that completes when the transaction is ready to be committed
    * @return
    */
  def awaitCommit(): Future[Unit] = {
    awaitingCommit = true
    //Check if the promise should be completed
    this.synchronized {
      if (pendingEvents == 0 && !awaitCommitPromise.isCompleted) {
        awaitCommitPromise.success()
      }
    }
    awaitCommitPromise.future
  }

  /**
    * Obtains a human-readible string representing the offsets each on a seperate line
    * @return
    */
  def displayOffsets(): String =
    offsetMap.map(tpo => s"(${tpo._1} -> ${tpo._2})").mkString("\r\n")

}

object transactionStateSerializer extends TypeSerializerSingleton[TransactionState] {
  override def createInstance(): TransactionState = new TransactionState()

  override def canEqual(obj: scala.Any): Boolean = obj == this

  override def serialize(record: TransactionState, target: DataOutputView): Unit = {
    target.writeInt(record.producerIndex)
    target.writeLong(record.checkPointId)
    target.writeInt(record.pendingEvents)
    target.writeBoolean(record.awaitingCommit)
    target.writeInt(record.offsetMap.size)
    record.offsetMap.foreach((tpo) => {
      target.writeInt(tpo._1)
      target.writeLong(tpo._2)
    })

  }

  override def isImmutableType: Boolean = false

  override def getLength: Int = -1

  override def copy(from: TransactionState): TransactionState = copy(from, new TransactionState())

  override def copy(from: TransactionState, reuse: TransactionState): TransactionState = {
    reuse.producerIndex = from.producerIndex
    reuse.checkPointId = from.checkPointId
    reuse.pendingEvents = from.pendingEvents
    reuse.awaitingCommit = from.awaitingCommit
    reuse.offsetMap = from.offsetMap.clone().asInstanceOf[mutable.Map[Int, Long]]
    reuse
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    target.writeInt(source.readInt())
    target.writeLong(source.readLong())
    target.writeInt(source.readInt())
    target.writeBoolean(source.readBoolean())
    val size = source.readInt()
    target.writeInt(size)
    (1 to size).foreach(_ => {
      target.writeUTF(source.readUTF())
      target.writeInt(source.readInt())
      target.writeLong(source.readLong())
    })
  }

  /**
    * Deserialize to a new state
    * @param source
    * @return
    */
  override def deserialize(source: DataInputView): TransactionState =
    deserialize(new TransactionState(), source)

  /**
    * TODO: Actually suport reuse
    * @param reuse
    * @param source
    * @return
    */
  override def deserialize(reuse: TransactionState, source: DataInputView): TransactionState = {
    reuse.producerIndex = source.readInt()
    reuse.checkPointId = source.readLong()
    reuse.pendingEvents = source.readInt()
    reuse.awaitingCommit = source.readBoolean()
    val size = source.readInt()

    reuse.offsetMap = mutable.Map() ++ (1 to size).map(_ => {
      val p = source.readInt()
      val o = source.readLong()
      p -> o
    })
    reuse
  }
}
