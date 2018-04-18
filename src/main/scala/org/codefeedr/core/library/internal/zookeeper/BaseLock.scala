package org.codefeedr.core.library.internal.zookeeper

import org.codefeedr.core.library.LibraryServices

import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Base class for read and write locks
  */
abstract class BaseLock {
  @transient protected lazy val zkClient = new ZkClient
  protected val promise = Promise[Unit]()
  protected val connector = zkClient.getConnector()
  var isOpen: Boolean = false
  //Make sure to put open on true
  promise.future.onComplete(_ => isOpen = true)

  /**
    * Closes the readlock, properly disposing of the lock, releasing it for other candidates
    * @return
    */
  def close(): Unit = {
    isOpen = false
  }

  /**
    * Returns a promise that resolves when a the lock is opened
    * @return
    */
  def awaitOpen(): Future[Unit] = promise.future

  /**
    * Obtains the sequence number from a sequention ZK node
    * Assumes the nodename without sequence ends on "-"
    * Currently unchecked
    * @param nodeName name of the node containing the sequence number
    * @return
    */
  protected def getSequence(nodeName: String): Long = nodeName.split("-").last.toLong

  /**
    * @param nodeName path of the node
    * @return boolean if the given path is a write lock node
    */
  protected def isWriteLock(nodeName: String): Boolean =
    nodeName.split("/").last.startsWith("write-")

  /**
    *
    * @param nodeName path of the node
    * @return boolean if the given path is a writelock node
    */
  protected def isReadLock(nodeName: String): Boolean =
    nodeName.split("/").last.startsWith("lock-")

  /**
    *
    * @param nodeName path of the node
    * @return boolean if the given path is a locknode
    */
  protected def isLock(nodeName: String): Boolean = isWriteLock(nodeName) || isReadLock(nodeName)
}
