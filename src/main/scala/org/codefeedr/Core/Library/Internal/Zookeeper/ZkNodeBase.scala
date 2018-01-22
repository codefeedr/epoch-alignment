package org.codefeedr.Core.Library.Internal.Zookeeper

import scala.concurrent.Future
import scala.reflect.ClassTag

class ZkNodeBase(zkClient: ZkClient)(path: String) {
  /**
    * Name of the node
    */
  val Name: String = path.split('/').last

  /**
    * Creates the node on zookeeper
    *
    * @return a future of the path used
    */
  def Create(): Future[String] = zkClient.Create(path).map(_ => path)

  /**
    * Get all children of a node
    *
    * @return
    */
  def GetChildren(): Future[Iterable[ZkNodeBase]] =
    zkClient.GetChildren(path).map(o => o.map(p => new ZkNodeBase(zkClient)(p)))

  /**
    * Creates a future that awaits the registration of a specific child
    *
    * @param child name of the child to await
    * @return a future that resolves when the child has been created, with the name of the child
    */
  def AwaitChild(child: String): Future[String] = zkClient.AwaitChild(path, child)

  /**
    * Creates a future that resolves whenever the node has been deleted from zookeeper
    *
    * @return the future
    */
  def AwaitRemoval(): Future[Unit] = zkClient.AwaitRemoval(path)

  /**
    * Checks if the node exists
    *
    * @return a future with the result
    */
  def Exists(): Future[Boolean] = zkClient.Exists(path)

  /**
    * Delete the current node
    *
    * @return a future that resolves when the node has been deleted
    */
  def Delete(): Future[Unit] = zkClient.Delete(path)

  /**
    * Delete the current node and all its children
    *
    * @return a future that resolves when the node has been deleted
    */
  def DeleteRecursive(): Future[Unit] = zkClient.DeleteRecursive(path)

  /**
    * Converts the current instance of zkNode to a typed node
    * @tparam TData
    * @return
    */
  def As[TData: ClassTag]: ZkNode[TData] = ZkNode[TData](path)
}
