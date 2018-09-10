package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper._

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.immutable
import scala.concurrent.Future

trait SubjectCollectionNode extends ZkCollectionNode[SubjectNode,Unit] {

  /**
    * Retrieves the current set of registered subject names
    *
    * @return A future with the set of registered subjects
    */
  def getNames(): Future[Set[String]]
}


trait SubjectCollectionNodeComponent extends ZkCollectionNodeComponent {
    this:ZkClientComponent
    with SubjectNodeComponent  =>
class SubjectCollectionNodeImpl(parent: ZkNodeBase)
  extends ZkCollectionNodeImpl[SubjectNode, Unit]("Subjects",
    parent,
    (name, parent) => new SubjectNodeImpl(name, parent)) with SubjectCollectionNode {

  /**
    * Retrieves the current set of registered subject names
    * @return A future with the set of registered subjects
    */
  override def getNames(): Future[immutable.Set[String]] = async {
    await(getChildren()).map(o => o.name).toSet
  }
}
}
