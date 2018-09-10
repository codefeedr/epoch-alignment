package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper._
import org.codefeedr.model.zookeeper.Producer

import scala.concurrent.Future


trait ProducerCollection extends ZkCollectionStateNode[ProducerNode, Unit, Producer, Boolean, Boolean] {

  def initial(): Boolean

  def mapChild(child: Boolean): Boolean

  def reduceAggregate(left: Boolean, right: Boolean): Boolean
}


trait ProducerCollectionNodeComponent extends ZkCollectionStateNodeComponent {
  this:ZkClientComponent
  with ProducerNodeComponent
  =>

  class ProducerCollectionImpl(subjectName: String, parent: ZkNodeBase)
    extends ZkCollectionNodeImpl[ProducerNode, Unit]("producers",
      parent,
      (name, parent) => new ProducerNodeImpl(name, parent))
      with ZkCollectionStateNodeImpl[ProducerNode, Unit, Producer, Boolean, Boolean] with ProducerCollection {

    override def initial(): Boolean = false

    override def mapChild(child: Boolean): Boolean = child

    override def reduceAggregate(left: Boolean, right: Boolean): Boolean = left || right

  }

}