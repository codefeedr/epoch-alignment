package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.{StateNode, ZkNode, ZkNodeBase}
import org.codefeedr.Model.Zookeeper.{QuerySink, QuerySource}

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class QuerySourceNode(name: String, parent: ZkNodeBase)
  extends ZkNode[QuerySource](name, parent)
  with StateNode[QuerySource] {
  override def PostCreate(): Future[Unit] = async {
    await(GetState().Create(true))
  }

  def GetConsumers(): ConsumerCollection = new ConsumerCollection("consumers",this)

}
