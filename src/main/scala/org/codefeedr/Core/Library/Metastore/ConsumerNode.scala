package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.{ZkStateNode, ZkNode, ZkNodeBase}
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
}
