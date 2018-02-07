package org.codefeedr.core.library.internal.zookeeper

import org.codefeedr.core.library.LibraryServices

import scala.async.Async.{async, await}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

abstract class ZkNodeBase(val name: String) {
  @transient lazy val zkClient: ZkClient = LibraryServices.zkClient

  def Parent(): ZkNodeBase

  def Path(): String = s"${Parent().Path()}/$name"

  /**
    * This method can be overridden to perform steps after the creation of the node
    * @return
    */
  def PostCreate(): Future[Unit] = Future.successful(Unit)

  /**
    * Creates the node on zookeeper
    *
    * @return a future of the path used
    */
  def Create(): Future[String] = async {
    await(zkClient.Create(Path()))
    await(PostCreate())
    Path()
  }

  /**
    * Checks if the node exists
    *
    * @return a future with the result
    */
  def Exists(): Future[Boolean] = zkClient.Exists(Path)

  /**
    * Delete the current node
    *
    * @return a future that resolves when the node has been deleted
    */
  def Delete(): Future[Unit] =
    Exists().flatMap(b => if (b) { zkClient.Delete(Path) } else { Future.successful() })

  /**
    * Delete the current node and all its children
    *
    * @return a future that resolves when the node has been deleted
    */
  def DeleteRecursive(): Future[Unit] =
    Exists().flatMap(b => if (b) { zkClient.DeleteRecursive(Path) } else { Future.successful() })

  /**
    * Creates a future that awaits the registration of a specific child
    *
    * @param child name of the child to await
    * @return a future that resolves when the child has been created, with the name of the child
    */
  def AwaitChild(child: String): Future[String] = zkClient.AwaitChild(Path, child)

  /**
    * Creates a future that resolves whenever the node has been deleted from zookeeper
    *
    * @return the future
    */
  def AwaitRemoval(): Future[Unit] = zkClient.AwaitRemoval(Path)
}
