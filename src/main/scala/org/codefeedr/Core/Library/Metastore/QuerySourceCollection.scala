package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.{
  ZkCollectionNode,
  ZkCollectionState,
  ZkNodeBase
}
import org.codefeedr.Model.Zookeeper.QuerySource

import scala.concurrent.Future

class QuerySourceCollection(parent: ZkNodeBase)
    extends ZkCollectionNode[QuerySourceNode]("sources",
                                              parent,
                                              (name, parent) => new QuerySourceNode(name, parent))
    with ZkCollectionState[QuerySourceNode, QuerySource, Boolean, Boolean] {

  override def Initial(): Boolean = false
  override def MapChild(child: Boolean): Boolean = child
  override def ReduceAggregate(left: Boolean, right: Boolean): Boolean = left || right

}
