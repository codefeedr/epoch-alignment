package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkStateNode, ZkNode, ZkNodeBase}
import org.codefeedr.Model.zookeeper.{Consumer, Producer}

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

class ProducerNode(name: String, parent: ZkNodeBase)
    extends ZkNode[Producer](name, parent)
    with ZkStateNode[Producer, Boolean] {

  override def TypeT(): ClassTag[Boolean] = ClassTag(classOf[Boolean])
  override def InitialState(): Boolean = true

  override def SetState(state: Boolean): Future[Unit] = async {
    await(super.SetState(state))
    await(parent.Parent().asInstanceOf[QuerySinkNode].UpdateState())
  }

}
