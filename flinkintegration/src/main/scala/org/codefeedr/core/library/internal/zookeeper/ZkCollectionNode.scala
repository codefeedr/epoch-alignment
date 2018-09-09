package org.codefeedr.core.library.internal.zookeeper

import com.typesafe.scalalogging.LazyLogging
import jdk.nashorn.internal.runtime.regexp.joni.ast.StateNode
import rx.lang.scala.Observable

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.ClassTag

class ZkCollectionNode[TNode <: ZkNodeBase, TData: ClassTag](
    name: String,
    p: ZkNodeBase,
    childConstructor: (String, ZkNodeBase) => TNode)(implicit override val zkClient: ZkClient)
    extends ZkNode[TData](name, p)
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
    //Hack: We should place waiting logic based on type of node elsewhere...
    zkClient
      .observeNewChildren(path())
      .map(o => childConstructor(o, this))
      .flatMap(o =>
        o match {
          case s: ZkStateNode[_, _] =>
            Observable.from(s.awaitChild(s.getStateNode().name)).map(o => s.asInstanceOf[TNode])
          case a => Observable.from(Future.successful(a))
      })
}
