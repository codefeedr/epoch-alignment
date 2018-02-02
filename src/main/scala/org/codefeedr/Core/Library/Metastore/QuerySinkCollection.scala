package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.{
  ZkCollectionNode,
  ZkCollectionStateNode,
  ZkNodeBase
}
import org.codefeedr.Model.Zookeeper.QuerySink

import scala.concurrent.Future

class QuerySinkCollection(parent: ZkNodeBase)
    extends ZkCollectionNode[QuerySinkNode]("sinks",
                                            parent,
                                            (name, parent) => new QuerySinkNode(name, parent))
    with ZkCollectionStateNode[QuerySinkNode, QuerySink, Boolean, Boolean] {

  override def Initial(): Boolean = false
  override def MapChild(child: Boolean): Boolean = child
  override def ReduceAggregate(left: Boolean, right: Boolean): Boolean = left || right

}
