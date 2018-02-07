package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{
  ZkCollectionNode,
  ZkCollectionStateNode,
  ZkNodeBase
}
import org.codefeedr.Model.zookeeper.QuerySink

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
