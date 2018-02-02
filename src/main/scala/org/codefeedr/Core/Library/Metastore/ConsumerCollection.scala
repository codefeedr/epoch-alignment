package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.{
  ZkCollectionNode,
  ZkCollectionStateNode,
  ZkNodeBase
}
import org.codefeedr.Model.Zookeeper.Consumer

class ConsumerCollection(subjectName: String, parent: ZkNodeBase)
    extends ZkCollectionNode[ConsumerNode]("consumers",
                                           parent,
                                           (name, parent) => new ConsumerNode(name, parent))
    with ZkCollectionStateNode[ConsumerNode, Consumer, Boolean, Boolean] {

  override def Initial(): Boolean = false
  override def MapChild(child: Boolean): Boolean = child
  override def ReduceAggregate(left: Boolean, right: Boolean): Boolean = left || right
}
