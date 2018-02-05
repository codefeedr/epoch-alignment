package org.codefeedr.Core.Library.Internal.Zookeeper

import com.typesafe.scalalogging.LazyLogging
import rx.lang.scala.Observable

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class ZkCollectionNode[TNode <: ZkNodeBase](name: String,
                                            val parent: ZkNodeBase,
                                            childConstructor: (String, ZkNodeBase) => TNode)
    extends ZkNodeBase(name)
    with LazyLogging {

  override def Parent(): ZkNodeBase = parent

  /**
    * Gets the child of the given name.
    * Does not validate if the name actually exists on zookeeper, just returns the node
    * @param name name of the child
    * @return
    */
  def GetChild(name: String): TNode = childConstructor(name, this)

  /**
    * Gets all childNodes currently located in zookeeper
    * @return
    */
  def GetChildren(): Future[Iterable[TNode]] =
    zkClient.GetChildren(Path()).map(o => o.map(GetChild))

  /**
    * Awaits child registration, and returns the node when the child has been created
    * @param child name of the child to await
    * @return a future that resolves when the child has been created, with the name of the child
    */
  def AwaitChildNode(child: String): Future[TNode] =
    AwaitChild(child).map(childConstructor(_, this))

  /**
    * Creates an observable of all new children
    * @return
    */
  def ObserveNewChildren(): Observable[TNode] =
    zkClient.ObserveNewChildren(Path()).map(o => childConstructor(o, this))
}
