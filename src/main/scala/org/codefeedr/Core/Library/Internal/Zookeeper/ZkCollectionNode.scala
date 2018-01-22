package org.codefeedr.Core.Library.Internal.Zookeeper

import scala.concurrent.Future
import scala.async.Async.{async, await}

class ZkCollectionNode[TElement](zkClient: ZkClient)(path: String) extends ZkNodeBase(zkClient)(path) {
  override def GetChildren(): Future[Iterable[ZkNode[TElement]]] =
    zkClient.GetChildren(path).map(o => o.map(ChildConstructor))


  protected def ChildConstructor(name : String) = ZkNode[TElement](s"$path/$name")
}
