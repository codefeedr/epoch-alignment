package org.codefeedr.core.library.internal.zookeeper

import com.typesafe.scalalogging.LazyLogging
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher}
import org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Class representing a readLock
  * Attempts to obtain a readLock upon creation
  * Please use this class within a managed block to properly call the close method after being done
  * No guaranteed behavior after calling close()
  * Following guideline on https://zookeeper.apache.org/doc/r3.1.2/recipes.html
  */
class ZkReadLock(path: String) extends BaseLock with LazyLogging {
  //Some local values
  private val lockCollectionNode = s"$path"

  //Create the locknode
  private var lockPath = connector.create(s"$lockCollectionNode/lock-",
                                          null,
                                          OPEN_ACL_UNSAFE,
                                          CreateMode.EPHEMERAL_SEQUENTIAL)
  private val sequence = getSequence(lockPath)
  logger.debug(s"$label created")

  private def label: String = s"Read lock on $path by sequence $sequence"

  checkIfLocked()

  /**
    * Performs a check if the current lock is open.
    * If not, places a recursive watch until it is open again
    */
  private def checkIfLocked(): Unit = {
    val children = connector.getChildren(lockCollectionNode, false).asScala
    val writeLocks = children.filter(isWriteLock)
    if (!writeLocks.exists(getSequence(_) < sequence)) {
      logger.debug(s"$label obtained lock")
      promise.success(())
    } else {
      logger.debug(s"$label for now waiting on lock")
      //Place a watch on the node that has a write lock
      val existsStat = connector.exists(
        s"$lockCollectionNode/${writeLocks.min(Ordering.by(getSequence))}",
        new Watcher {
          override def process(watchedEvent: WatchedEvent): Unit = checkIfLocked()
        })

      if (existsStat == null) {
        //Node has been removed in the meantime, so check again if lock can be obtained
        checkIfLocked()
      }
    }
  }

  /**
    * Closes the readlock, properly disposing of the lock, releasing it for other candidates
    * @return
    */
  override def close(): Unit = {
    super.close()
    connector.delete(lockPath, -1)
    logger.debug(s"$label released lock")
  }
}

object ZkReadLock {
  def open(path: String): Future[ZkReadLock] = {
    val lock = new ZkReadLock(path)
    lock.awaitOpen.map(_ => lock)
  }
}
