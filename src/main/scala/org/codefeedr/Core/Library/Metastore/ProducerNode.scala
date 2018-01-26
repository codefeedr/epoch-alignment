package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.{StateNode, ZkNode, ZkNodeBase}
import org.codefeedr.Model.Zookeeper.{Consumer, Producer}

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class ProducerNode(name: String, parent: ZkNodeBase)
  extends ZkNode[Producer](name, parent)
    with StateNode[Producer] {
  override def PostCreate(): Future[Unit] = async {
    await(GetState().Create(true))
  }

}
