package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.{ZkStateNode, ZkNode, ZkNodeBase}
import org.codefeedr.Model.Zookeeper.{QuerySink, QuerySource}

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

class QuerySourceNode(name: String, parent: ZkNodeBase)
    extends ZkNode[QuerySource](name, parent)
    with ZkStateNode[QuerySource, Boolean] {
  override def PostCreate(): Future[Unit] = async {
    await(GetStateNode().Create(true))
  }

  def GetConsumers(): ConsumerCollection = new ConsumerCollection("consumers", this)

  override def TypeT(): ClassTag[Boolean] = ClassTag(classOf[Boolean])
  override def InitialState(): Boolean = true
}
