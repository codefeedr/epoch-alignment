package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper._
import org.codefeedr.model.zookeeper.QuerySource

import scala.concurrent.Future

trait QuerySourceCollection extends ZkCollectionStateNode[QuerySourceNode, Unit, QuerySource, Boolean, Boolean] {

  def initial(): Boolean

  def mapChild(child: Boolean): Boolean

  def reduceAggregate(left: Boolean, right: Boolean): Boolean
}


trait QuerySourceCollectionComponent extends ZkCollectionStateNodeComponent {
  this:ZkClientComponent
  with QuerySourceNodeComponent =>

  class QuerySourceCollectionImpl(parent: ZkNodeBase)
    extends ZkCollectionNodeImpl[QuerySourceNode, Unit](
      "sources",
      parent,
      (name, parent) => new QuerySourceNodeImpl(name, parent))
      with ZkCollectionStateNodeImpl[QuerySourceNode, Unit, QuerySource, Boolean, Boolean] with QuerySourceCollection {

    override def initial(): Boolean = false

    override def mapChild(child: Boolean): Boolean = child

    override def reduceAggregate(left: Boolean, right: Boolean): Boolean = left || right

  }

}