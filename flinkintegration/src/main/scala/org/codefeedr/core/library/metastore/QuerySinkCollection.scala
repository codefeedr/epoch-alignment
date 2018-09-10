package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper._
import org.codefeedr.model.zookeeper.QuerySink

import scala.concurrent.Future

trait QuerySinkCollection extends ZkCollectionStateNode[QuerySinkNode, Unit, QuerySink, Boolean, Boolean] {

  def subject(): SubjectNode

  def initial(): Boolean

  def mapChild(child: Boolean): Boolean

  def reduceAggregate(left: Boolean, right: Boolean): Boolean
}


trait QuerySinkCollectionComponent extends ZkCollectionStateNodeComponent {
  this:ZkClientComponent
  with QuerySinkNodeComponent =>

  class QuerySinkCollectionImpl(parent: ZkNodeBase)
    extends ZkCollectionNodeImpl[QuerySinkNode, Unit](
      "sinks",
      parent,
      (name, parent) => new QuerySinkNodeImpl(name, parent))
      with ZkCollectionStateNodeImpl[QuerySinkNode, Unit, QuerySink, Boolean, Boolean] with QuerySinkCollection {

    override def subject(): SubjectNode = parent().asInstanceOf[SubjectNode]

    override def initial(): Boolean = false

    override def mapChild(child: Boolean): Boolean = child

    override def reduceAggregate(left: Boolean, right: Boolean): Boolean = left || right

  }

}