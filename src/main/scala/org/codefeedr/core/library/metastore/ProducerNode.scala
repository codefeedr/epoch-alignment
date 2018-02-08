package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkStateNode, ZkNode, ZkNodeBase}
import org.codefeedr.model.zookeeper.{Consumer, Producer}

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

class ProducerNode(name: String, parent: ZkNodeBase)
    extends ZkNode[Producer](name, parent)
    with ZkStateNode[Producer, Boolean] {

  override def typeT(): ClassTag[Boolean] = ClassTag(classOf[Boolean])
  override def initialState(): Boolean = true

  override def setState(state: Boolean): Future[Unit] = async {
    await(super.setState(state))
    await(parent.parent().asInstanceOf[QuerySinkNode].updateState())
  }

}
