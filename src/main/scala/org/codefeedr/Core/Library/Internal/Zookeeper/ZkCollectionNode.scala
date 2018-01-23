package org.codefeedr.Core.Library.Internal.Zookeeper

import scala.concurrent.Future
import scala.async.Async.{async, await}

class ZkCollectionNode[TNode <: ZkNodeBase](name: String, val parent: ZkNodeBase, childConstructor: (String, ZkNodeBase) => TNode)
  extends ZkNodeBase(name) {

  override def Parent(): ZkNodeBase = parent

  /**
    * Gets all childNodes currently located in zookeeper
    * @return
    */
  def GetChildren(): Future[Iterable[TNode]] =
    zkClient.GetChildren(Path()).map(o => o.map(GetChild))


  /**
    * Gets the child of the given name.
    * Does not validate if the name actually exists on zookeeper, just returns the node
    * @param name name of the child
    * @return
    */
  def GetChild(name : String): TNode = childConstructor(name)(this)
}
