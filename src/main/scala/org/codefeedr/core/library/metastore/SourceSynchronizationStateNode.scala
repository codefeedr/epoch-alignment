package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkNode, ZkNodeBase}
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future

/** Synchronization state.
  * 0: unsyncrhonized
  * 1: Catching up
  * 2: Synchronized
  */
case class SynchronizationState(state: Int)

class SourceSynchronizationStateNode(parent: ZkNodeBase)
    extends ZkNode[SynchronizationState]("syncstate", parent) {
  /*
  By default create it in the unsynchronized state
   */
  override def create(): Future[String] =
    super.create(SynchronizationState(0)).map(_ => "syncstate")
}
