package org.codefeedr.core.library.internal.zookeeper

import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE
import org.codefeedr.core.library.LibraryServices
import resource.Resource

import scala.concurrent.{Future, Promise}

/**
  * Class representing a readLock
  * Attempts to obtain a readLock upon creation
  * Please use this class within a managed block to properly call the close method after being done
  * Following guideline on https://zookeeper.apache.org/doc/r3.1.2/recipes.html
  */
class ZkReadLock(path: String) {
  //Some local values
  @transient val zkClient: ZkClient = LibraryServices.zkClient
  private val lockNodePath = s"$path/_locknode_"
  private val lockPath = s"$lockNodePath/lock-"
  private val connector = zkClient.getConnector()
  private val promise = Promise[Unit]()

  //Create the locknode
  connector.create(lockPath,null,OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL)


  /**
    * Performs a check if the current lock is open.
    * If not, places a recursive watch until it is open again
    */
  private def checkIfLocked(): Unit = {

  }




  /**
    * Closes the readlock, properly disposing of the lock, releasing it for other candidates
    * @return
    */
  def close():Unit  = {

  }

  /**
    * Returns a boolean if the lock is open
    * Might still return true after lock has already been disposed of!
    * @return
    */
  def isOpen: Boolean = promise.isCompleted

  /**
    * Returns a promise that resolves when a the lock is opened
    * @return
    */
  def awaitOpen(): Future[Unit] = promise.future
}
