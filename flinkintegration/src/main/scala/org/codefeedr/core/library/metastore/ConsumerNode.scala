package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{
  ZkClientComponent,
  ZkNode,
  ZkNodeBase,
  ZkStateNode
}
import org.codefeedr.model.zookeeper.Consumer

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

class ConsumerNode(name: String, parent: ZkNodeBase)
    extends ZkNode[Consumer](name, parent)
    with ZkStateNode[Consumer, Boolean] {
  override def postCreate(): Future[Unit] = {
    //HACK: Not really asynchronous, but needed because consumers are created from synchronous code
    getSyncState().createSync(null)
    super.postCreate()
  }

  def getSyncState(): SourceSynchronizationStateNode = new SourceSynchronizationStateNode(this)

  def consumerCollection(): ConsumerCollection = parent().asInstanceOf[ConsumerCollection]

  override def typeT(): ClassTag[Boolean] = ClassTag(classOf[Boolean])

  /**
    * The initial state of the node. State is not allowed to be empty
    *
    * @return
    */
  override def initialState(): Boolean = true

  override def setState(state: Boolean): Future[Unit] = async {
    await(super.setState(state))
    //Call update on query source state, because this consumer state change might impact it
    await(parent.parent().asInstanceOf[QuerySourceNode].updateState())
  }
}
