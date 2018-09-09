package org.codefeedr.core.library.internal.zookeeper

import com.typesafe.scalalogging.LazyLogging

import scala.async.Async.{async, await}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Non-generic base class for all ZkNodes
  * @param name
  */
abstract class ZkNodeBase(val name: String)(implicit val zkClient: ZkClient) extends Serializable with LazyLogging {


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
    await(zkClient.create(path()))
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
    exists().flatMap(b => if (b) { zkClient.Delete(path) } else { Future.successful(()) })

  /**
    * Delete the current node and all its children
    *
    * @return a future that resolves when the node has been deleted
    */
  def deleteRecursive(): Future[Unit] =
    exists().flatMap(b => if (b) { zkClient.deleteRecursive(path) } else { Future.successful(()) })

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

  /**
    * Creates a readLock on the given path
    * Attempts to obtain lock from the moment of calling
    * Please use the returned object as managed resource to prevent deadlocks
    * @return future that resolves to the writeLock for the node
    */
  def readLock(): Future[ZkReadLock] = zkClient.readLock(path)

  /**
    * Performs the given asynchronous method within a read lock on the node
    * @param a the asynchronous operation to perform
    * @tparam TResult result type of the asynchronous operation
    * @return
    */
  def asyncReadLock[TResult](a: () => Future[TResult]): Future[TResult] = async {
    val lock = await(readLock())
    await(try { //Make sure to catch any exception during the creation of the future
      val f = a()
      //Close lock when it completes
      f.onComplete(_ => lock.close())
      f
    } catch {
      case e: Exception => {
        lock.close()
        throw e
      }
    })
  }

  /**
    * Creates a writelock on the given path
    * Attempts to obtain lock from the moment of calling
    * Please use the returned object as managed resource to prevent deadlocks
    * @return future that resolves to the writeLock for the node
    */
  def writeLock(): Future[ZkWriteLock] = zkClient.writeLock(path)

  /**
    * Performs the given asynchronous method within a write lock on the node
    * @param a the asynchronous operation to perform
    * @tparam TResult result type of the asynchronous operation
    * @return
    */
  def asyncWriteLock[TResult](a: () => Future[TResult]): Future[TResult] = async {
    val lock = await(writeLock())
    await(try { //Make sure to catch any exception during the creation of the future
      val f = a()
      //Close lock when it completes
      f.onComplete(_ => lock.close())
      f
    } catch {
      case e: Exception => {
        lock.close()
        throw e
      }
    })
  }

}
