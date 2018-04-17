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
    * @return
    */
  def getLatestEpochId(): Future[Int] = async {
    val epochs = await(getChildren())
    if (epochs.nonEmpty) {
      logger.info(s"TODO: Implement epoch")
      epochs.map(o => o.getEpoch()).max
      -1
    } else {
      -1
    }
  }

  def getChild(epoch: Int): EpochNode = {
    super.getChild(s"$epoch")
  }
}
