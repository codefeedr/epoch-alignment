package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.{ZkClient, ZkCollectionNode, ZkNodeBase}

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global

import scala.collection.immutable
import scala.concurrent.Future

class SubjectCollectionNode(parent: ZkNodeBase)
    extends ZkCollectionNode[SubjectNode]("Subjects",
                                          parent,
                                          (name, parent) => new SubjectNode(name, parent)) {

  /**
    * Retrieves the current set of registered subject names
    * @return A future with the set of registered subjects
    */
  def GetNames(): Future[immutable.Set[String]] = async {
    await(GetChildren()).map(o => o.name).toSet
  }
}
