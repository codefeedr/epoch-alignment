package org.codefeedr.core.library.internal.zookeeper

import org.codefeedr.core.library.LibraryServices

import scala.async.Async.{async, await}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

abstract class ZkNodeBase(val name: String) {
  @transient lazy val zkClient: ZkClient = LibraryServices.zkClient

  def parent(): ZkNodeBase

  def path(): String = s"${parent().path()}/$name"

  /**
    * This method can be overridden to perform steps after the creation of the node
    * @return
    */
  def postCreate(): Future[Unit] = Future.successful(Unit)

  /**
    * Creates the node on zookeeper
    *
    * @return a future of the path used
    */
  def create(): Future[String] = async {
    await(zkClient.Create(path()))
    await(postCreate())
    path()
  }

  /**
    * Checks if the node exists
    *
    * @return a future with the result
    */
  def exists(): Future[Boolean] = zkClient.exists(path)

  /**
    * Delete the current node
    *
    * @return a future that resolves when the node has been deleted
    */
  def delete(): Future[Unit] =
    exists().flatMap(b => if (b) { zkClient.Delete(path) } else { Future.successful() })

  /**
    * Delete the current node and all its children
    *
    * @return a future that resolves when the node has been deleted
    */
  def deleteRecursive(): Future[Unit] =
    exists().flatMap(b => if (b) { zkClient.deleteRecursive(path) } else { Future.successful() })

  /**
    * Creates a future that awaits the registration of a specific child
    *
    * @param child name of the child to await
    * @return a future that resolves when the child has been created, with the name of the child
    */
  def awaitChild(child: String): Future[String] = zkClient.awaitChild(path, child)

  /**
    * Creates a future that resolves whenever the node has been deleted from zookeeper
    *
    * @return the future
    */
  def awaitRemoval(): Future[Unit] = zkClient.awaitRemoval(path)
}
