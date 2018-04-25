package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkCollectionNode, ZkNodeBase}

import scala.async.Async.{async, await}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Epoch collection node
  * Contains epoch mappings to offset pet job
  * @param parent
  */
class EpochCollectionNode(parent: ZkNodeBase)
    extends ZkCollectionNode[EpochNode]("epochs", parent, (n, p) => new EpochNode(n.toInt, p)) {

  /**
    * Retrieves the latest known completed checkpoint for this subject.
    * returns -1 if the subject has no checkpoints.
    * If the subject is still active, this method might return different values upon each call
    * TODO: This methods needs to be optimized to not retrieve the state of all epochs so far
    * @return
    */
  def getLatestEpochId(): Future[Int] = async {
    val epochs = await(getChildren())
    //TODO: Optimize this sorting and filtering with alot of futures
    val sorted = epochs.toList.sortBy(o => o.getEpoch()).reverse
    val filtered = await(Future.sequence(sorted.map(o =>
      async {
        o.getEpoch() -> await(o.getState()).get
    }))).filter(o => o._2)
    if (filtered.nonEmpty) {
      val head = filtered.head._1
      logger.info(s"Latest epoch was: $head")
      head
    } else {
      if (epochs.nonEmpty) {
        logger.warn(s"Return -1 as latest epoch because none of the epochs known were completed")
      }
      -1
    }
  }

  def getChild(epoch: Long): EpochNode = {
    super.getChild(s"$epoch")
  }
}
