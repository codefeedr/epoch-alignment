package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{
  ZkClient,
  ZkCollectionNode,
  ZkCollectionStateNode,
  ZkNodeBase
}
import org.codefeedr.model.zookeeper.Producer

import scala.concurrent.Future

class ProducerCollection(subjectName: String, parent: ZkNodeBase)
    extends ZkCollectionNode[ProducerNode]("producers",
                                           parent,
                                           (name, parent) => new ProducerNode(name, parent))
    with ZkCollectionStateNode[ProducerNode, Producer, Boolean, Boolean] {

  override def initial(): Boolean = false
  override def mapChild(child: Boolean): Boolean = child
  override def reduceAggregate(left: Boolean, right: Boolean): Boolean = left || right

}
