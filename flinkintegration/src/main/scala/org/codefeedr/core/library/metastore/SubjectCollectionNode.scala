package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkClient, ZkCollectionNode, ZkNodeBase}

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global

import scala.collection.immutable
import scala.concurrent.Future

class SubjectCollectionNode(parent: ZkNodeBase)(implicit override val zkClient: ZkClient)
    extends ZkCollectionNode[SubjectNode, Unit]("Subjects",
                                                parent,
                                                (name, parent) => new SubjectNode(name, parent)) {

  /**
    * Retrieves the current set of registered subject names
    * @return A future with the set of registered subjects
    */
  def getNames(): Future[immutable.Set[String]] = async {
    await(getChildren()).map(o => o.name).toSet
  }
}
