package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{
  ZkCollectionNode,
  ZkCollectionStateNode,
  ZkNodeBase
}
import org.codefeedr.model.zookeeper.QuerySink

import scala.concurrent.Future

class QuerySinkCollection(parent: ZkNodeBase)
    extends ZkCollectionNode[QuerySinkNode, Unit](
      "sinks",
      parent,
      (name, parent) => new QuerySinkNode(name, parent))
    with ZkCollectionStateNode[QuerySinkNode, Unit, QuerySink, Boolean, Boolean] {

  override def initial(): Boolean = false
  override def mapChild(child: Boolean): Boolean = child
  override def reduceAggregate(left: Boolean, right: Boolean): Boolean = left || right

}
