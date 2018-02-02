package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.{ZkStateNode, ZkNode, ZkNodeBase}
import org.codefeedr.Model.Zookeeper.{Producer, QuerySink}

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

class QuerySinkNode(name: String, parent: ZkNodeBase)
    extends ZkNode[QuerySink](name, parent)
    with ZkStateNode[QuerySink, Boolean] {
  override def PostCreate(): Future[Unit] = async {
    await(GetStateNode().Create(true))
  }

  def GetProducers(): ProducerCollection = new ProducerCollection("producers", this)

  override def TypeT(): ClassTag[Boolean] = ClassTag(classOf[Boolean])
  override def InitialState(): Boolean = true
}
