package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.{StateNode, ZkNode, ZkNodeBase}
import org.codefeedr.Model.Zookeeper.{Producer, QuerySink}

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class QuerySinkNode(name: String, parent: ZkNodeBase)
  extends ZkNode[QuerySink](name, parent)
  with StateNode[QuerySink] {
  override def PostCreate(): Future[Unit] = async {
    await(GetState().Create(true))
  }



  def GetProducers(): ProducerCollection = new ProducerCollection("producers",this)
}
