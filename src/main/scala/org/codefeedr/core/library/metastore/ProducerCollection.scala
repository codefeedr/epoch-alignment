package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{
  ZkClient,
  ZkCollectionNode,
  ZkCollectionStateNode,
  ZkNodeBase
}
import org.codefeedr.Model.zookeeper.Producer

import scala.concurrent.Future

class ProducerCollection(subjectName: String, parent: ZkNodeBase)
    extends ZkCollectionNode[ProducerNode]("producers",
                                           parent,
                                           (name, parent) => new ProducerNode(name, parent))
    with ZkCollectionStateNode[ProducerNode, Producer, Boolean, Boolean] {

  override def Initial(): Boolean = false
  override def MapChild(child: Boolean): Boolean = child
  override def ReduceAggregate(left: Boolean, right: Boolean): Boolean = left || right

}
