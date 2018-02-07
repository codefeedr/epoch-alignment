package org.codefeedr.core.Library.Metastore

import org.codefeedr.core.Library.Internal.Zookeeper.{ZkStateNode, ZkNode, ZkNodeBase}
import org.codefeedr.Model.Zookeeper.Consumer

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

class ConsumerNode(name: String, parent: ZkNodeBase)
    extends ZkNode[Consumer](name, parent)
    with ZkStateNode[Consumer, Boolean] {
  override def PostCreate(): Future[Unit] = async {
    await(GetStateNode().Create(true))
  }

  override def TypeT(): ClassTag[Boolean] = ClassTag(classOf[Boolean])

  /**
    * The initial state of the node. State is not allowed to be empty
    *
    * @return
    */
  override def InitialState(): Boolean = true

  override def SetState(state: Boolean): Future[Unit] = async {
    await(super.SetState(state))
    //Call update on query source state, because this consumer state change might impact it
    await(parent.Parent().asInstanceOf[QuerySourceNode].UpdateState())
  }
}
