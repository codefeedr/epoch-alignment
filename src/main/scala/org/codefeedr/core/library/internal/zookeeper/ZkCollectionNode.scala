package org.codefeedr.core.library.internal.zookeeper

import com.typesafe.scalalogging.LazyLogging
import rx.lang.scala.Observable

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class ZkCollectionNode[TNode <: ZkNodeBase](name: String,
                                            val p: ZkNodeBase,
                                            childConstructor: (String, ZkNodeBase) => TNode)
    extends ZkNodeBase(name)
    with LazyLogging {

  override def parent(): ZkNodeBase = p

  /**
    * Gets the child of the given name.
    * Does not validate if the name actually exists on zookeeper, just returns the node
    * @param name name of the child
    * @return
    */
  def getChild(name: String): TNode = childConstructor(name, this)

  /**
    * Gets all childNodes currently located in zookeeper
    * @return
    */
  def getChildren(): Future[Iterable[TNode]] =
    zkClient.GetChildren(path()).map(o => o.map(getChild))

  /**
    * Awaits child registration, and returns the node when the child has been created
    * @param child name of the child to await
    * @return a future that resolves when the child has been created, with the name of the child
    */
  def awaitChildNode(child: String): Future[TNode] =
    awaitChild(child).map(childConstructor(_, this))

  /**
    * Creates an observable of all new children
    * @return
    */
  def observeNewChildren(): Observable[TNode] =
    zkClient.observeNewChildren(path()).map(o => childConstructor(o, this))
}
