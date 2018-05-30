package org.codefeedr.core.library.internal.zookeeper

import com.typesafe.scalalogging.LazyLogging
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher}
import org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class ZkWriteLock(path: String) extends BaseLock with LazyLogging {
  //Some local values
  private val lockCollectionNode = s"$path"

  //Create the locknode
  private val lockPath = connector.create(s"$lockCollectionNode/write-",
                                          null,
                                          OPEN_ACL_UNSAFE,
                                          CreateMode.EPHEMERAL_SEQUENTIAL)
  private val sequence = getSequence(lockPath)
  logger.debug(s"$label created")

  //Start attempting to obtain a lock
  checkIfLocked()

  private def label: String = s"Write lock on $path by sequence $sequence"

  /**
    * Performs a check if the current lock is open.
    * If not, places a recursive watch until it is open again
    */
  private def checkIfLocked(): Unit = {
    logger.debug(s"$label obtaining lock children")
    val children = connector.getChildren(lockCollectionNode, false).asScala
    logger.debug(s"$label obtained lock children")
    val locks = children.filter(isLock)
    if (!locks.exists(getSequence(_) < sequence)) {
      logger.debug(s"$label obtained lock")
      promise.success(())
    } else {
      logger.debug(s"$label for now waiting on lock")
      //Place a watch on the node that has a write lock
      val existsStat = connector.exists(
        s"$lockCollectionNode/${locks.min(Ordering.by(getSequence))}",
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

object ZkWriteLock {
  def open(path: String): Future[ZkWriteLock] = {
    val lock = new ZkWriteLock(path)
    lock.awaitOpen.map(_ => lock)
  }
}
