package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.kafka.source.KafkaSourceState
import org.codefeedr.core.library.internal.zookeeper.{ZkClient, ZkNode, ZkNodeBase}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Synchronization state.
  */
case class SynchronizationState(state: KafkaSourceState.Value)

class SourceSynchronizationStateNode(parent: ZkNodeBase)(implicit override val zkClient: ZkClient)
    extends ZkNode[SynchronizationState]("syncstate", parent) {
  /*
  By default create it in the unsynchronized state
   */
  override def create(): Future[String] =
    super.create(SynchronizationState(KafkaSourceState.UnSynchronized)).map(_ => "syncstate")
}
