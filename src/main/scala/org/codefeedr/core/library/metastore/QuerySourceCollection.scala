package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{
  ZkCollectionNode,
  ZkCollectionStateNode,
  ZkNodeBase
}
import org.codefeedr.Model.zookeeper.QuerySource

import scala.concurrent.Future

class QuerySourceCollection(parent: ZkNodeBase)
    extends ZkCollectionNode[QuerySourceNode]("sources",
                                              parent,
                                              (name, parent) => new QuerySourceNode(name, parent))
    with ZkCollectionStateNode[QuerySourceNode, QuerySource, Boolean, Boolean] {

  override def Initial(): Boolean = false
  override def MapChild(child: Boolean): Boolean = child
  override def ReduceAggregate(left: Boolean, right: Boolean): Boolean = left || right

}
