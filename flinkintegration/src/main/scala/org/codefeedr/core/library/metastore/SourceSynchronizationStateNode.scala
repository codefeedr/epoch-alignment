package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.kafka.source.KafkaSourceState
import org.codefeedr.core.library.internal.zookeeper._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Synchronization state.
  */
case class SynchronizationState(state: KafkaSourceState.Value)

trait SourceSynchronizationStateNode extends ZkNode[SynchronizationState]

trait SourceSynchronizationStateNodeComponent extends ZkNodeComponent { this: ZkClientComponent =>

  class SourceSynchronizationStateNodeImpl(parent: ZkNodeBase)
      extends ZkNodeImpl[SynchronizationState]("syncstate", parent)
      with SourceSynchronizationStateNode {
    /*
  By default create it in the unsynchronized state
     */
    override def create(): Future[String] =
      super.create(SynchronizationState(KafkaSourceState.UnSynchronized)).map(_ => "syncstate")
  }

}
