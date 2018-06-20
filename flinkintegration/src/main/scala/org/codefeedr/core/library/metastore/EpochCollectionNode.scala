package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkCollectionNode, ZkNode, ZkNodeBase}
import org.codefeedr.model.zookeeper.EpochCollection

import scala.async.Async.{async, await}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Epoch collection node
  * Contains epoch mappings to offset pet job
  * @param parent
  */
class EpochCollectionNode(parent: ZkNodeBase)
    extends ZkCollectionNode[EpochNode, EpochCollection]("epochs",
                                                         parent,
                                                         (n, p) => new EpochNode(n.toInt, p)) {

  /**
    * Retrieves the latest known completed checkpoint for this subject.
    * returns -1 if the subject has no checkpoints.
    * If the subject is still active, this method might return different values upon each call
    * @return
    */
  def getLatestEpochId: Future[Long] = async {
    getLatestEpochId(await(getData()))
  }

  /**
    * Retrieves the latest known completed checkpoint for this subject.
    * returns -1 if the subject has no checkpoints.
    * If the subject is still active, this method might return different values upon each call
    * @return
    */
  def getLatestEpochIdSync: Long = getLatestEpochId(getDataSync())

  private def getLatestEpochId(data: Option[EpochCollection]): Long = data match {
    case None => throw new IllegalStateException(s"No latest epoch known for subject $parent")
    case Some(v) =>
      logger.debug(s"Got latest epoch: ${v.latestEpoch}")
      v.latestEpoch
  }

  def getChild(epoch: Long): EpochNode = {
    super.getChild(s"$epoch")
  }

}
