package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.{
  ZkClient,
  ZkCollectionNode,
  ZkCollectionState,
  ZkNodeBase
}
import org.codefeedr.Model.Zookeeper.Producer

import scala.concurrent.Future

class ProducerCollection(subjectName: String, parent: ZkNodeBase)
    extends ZkCollectionNode[ProducerNode]("producers",
                                           parent,
                                           (name, parent) => new ProducerNode(name, parent))
    with ZkCollectionState[ProducerNode, Producer, Boolean, Boolean] {

  override def Initial(): Boolean = false
  override def MapChild(child: Boolean): Boolean = child
  override def ReduceAggregate(left: Boolean, right: Boolean): Boolean = left || right

}
