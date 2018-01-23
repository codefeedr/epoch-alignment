package org.codefeedr.Core.Library.Internal.Zookeeper

import org.codefeedr.Core.Library.LibraryServices

import scala.concurrent.Future
import scala.reflect.ClassTag

abstract class ZkNodeBase(val name: String) {
  @transient lazy val zkClient: ZkClient = LibraryServices.zkClient

  abstract def Parent(): ZkNodeBase

  def Path(): String = s"${Parent().Path()}/$name"

  /**
    * Creates the node on zookeeper
    *
    * @return a future of the path used
    */
  def Create(): Future[String] = zkClient.Create(Path).map(_ => Path)

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
  def Delete(): Future[Unit] = zkClient.Delete(Path)

  /**
    * Delete the current node and all its children
    *
    * @return a future that resolves when the node has been deleted
    */
  def DeleteRecursive(): Future[Unit] = zkClient.DeleteRecursive(Path)

  /**
    * Converts the current instance of zkNode to a typed node
    * @tparam TData
    * @return
    */
  def As[TData: ClassTag]: ZkNode[TData] = ZkNode[TData](Path)
}
