package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{
  ZkCollectionNode,
  ZkCollectionStateNode,
  ZkNodeBase
}
import org.codefeedr.model.zookeeper.QuerySource

import scala.concurrent.Future

class QuerySourceCollection(parent: ZkNodeBase)
    extends ZkCollectionNode[QuerySourceNode,Unit]("sources",
                                              parent,
                                              (name, parent) => new QuerySourceNode(name, parent))
    with ZkCollectionStateNode[QuerySourceNode,Unit, QuerySource, Boolean, Boolean] {

  override def initial(): Boolean = false
  override def mapChild(child: Boolean): Boolean = child
  override def reduceAggregate(left: Boolean, right: Boolean): Boolean = left || right

}
