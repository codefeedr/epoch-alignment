package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{
  ZkClientComponent,
  ZkCollectionNode,
  ZkCollectionStateNode,
  ZkNodeBase
}
import org.codefeedr.model.zookeeper.Consumer

class ConsumerCollection(subjectName: String, parent: ZkNodeBase)
    extends ZkCollectionNode[ConsumerNode]("consumers",
                                           parent,
                                           (name, parent) => new ConsumerNode(name, parent))
    with ZkCollectionStateNode[ConsumerNode, Consumer, Boolean, Boolean] {

  override def initial(): Boolean = false
  override def mapChild(child: Boolean): Boolean = child
  override def reduceAggregate(left: Boolean, right: Boolean): Boolean = left || right
}
