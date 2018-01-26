package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.{StateNode, ZkNode, ZkNodeBase}
import org.codefeedr.Model.Zookeeper.Consumer

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class ConsumerNode(name: String, parent: ZkNodeBase)
  extends ZkNode[Consumer](name, parent)
  with StateNode[Consumer] {
  override def PostCreate(): Future[Unit] = async {
    await(GetState().Create(true))
  }

}
