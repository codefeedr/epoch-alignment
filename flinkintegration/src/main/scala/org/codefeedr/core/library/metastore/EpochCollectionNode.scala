package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper._
import org.codefeedr.model.zookeeper.EpochCollection

import scala.async.Async.{async, await}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global


trait EpochCollectionNode extends ZkCollectionNode[EpochNode, EpochCollection]{

  /**
    * Retrieves the latest known completed checkpoint for this subject.
    * returns -1 if the subject has no checkpoints.
    * If the subject is still active, this method might return different values upon each call
    *
    * @return
    */
  def getLatestEpochId: Future[Long]

  def getChild(epoch: Long): EpochNode
}


trait EpochCollectionNodeComponent extends ZkCollectionNodeComponent {
  this: ZkClientComponent
  with EpochNodeComponent =>

  /**
    * Epoch collection node
    * Contains epoch mappings to offset pet job
    *
    * @param parent
    */
  class EpochCollectionNodeImpl(parent: ZkNodeBase)
    extends ZkCollectionNodeImpl[EpochNode, EpochCollection]("epochs",
      parent,
      (n, p) => new EpochNodeImpl(n.toInt, p)) with EpochCollectionNode {

    /**
      * Retrieves the latest known completed checkpoint for this subject.
      * returns -1 if the subject has no checkpoints.
      * If the subject is still active, this method might return different values upon each call
      *
      * @return
      */
    override def getLatestEpochId: Future[Long] = async {
      val data = await(getData())
      data match {
        case None => throw new IllegalStateException(s"No latest epoch known for subject $parent")
        case Some(v) =>
          logger.debug(s"Got latest epoch: ${v.latestEpoch}")
          v.latestEpoch
      }
    }

    override def getChild(epoch: Long): EpochNode = {
      super.getChild(s"$epoch")
    }
  }

}
