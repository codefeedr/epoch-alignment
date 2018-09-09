package org.codefeedr.core.library.metastore
import org.codefeedr.core.library.internal.zookeeper.{ZkClient, ZkCollectionNode, ZkNodeBase}
import org.codefeedr.model.zookeeper.EpochCollection

import scala.async.Async.{async, await}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
class SourceEpochCollection(parent: ZkNodeBase)(implicit override val zkClient: ZkClient)
    extends ZkCollectionNode[SourceEpochNode, EpochCollection](
      "epochs",
      parent,
      (n, p) => new SourceEpochNode(n.toInt, p)) {

  /**
    * Retrieves the latest known completed checkpoint for this source.
    * returns -1 if the source has no checkpoints.
    * If the subject is still active, this method might return different values upon each call
    * @return
    */
  def getLatestEpochId(): Future[Long] = async {
    await(getData()).get.latestEpoch
  }

  /**
    * Retrieve the epochNode of the given epcoh
    * @param e
    * @return
    */
  def getEpoch(e: Int): SourceEpochNode = new SourceEpochNode(e, this)
}
