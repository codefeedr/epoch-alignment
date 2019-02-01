package org.codefeedr.core.library.metastore
import org.codefeedr.core.library.internal.zookeeper._
import org.codefeedr.model.zookeeper.EpochCollection

import scala.async.Async.{async, await}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

trait SourceEpochCollection extends ZkCollectionNode[SourceEpochNode, EpochCollection] {

  /**
    * Retrieves the latest known completed checkpoint for this source.
    * returns -1 if the source has no checkpoints.
    * If the subject is still active, this method might return different values upon each call
    *
    * @return
    */
  def getLatestEpochId(): Future[Long]

  /**
    * Retrieve the epochNode of the given epcoh
    *
    * @param e
    * @return
    */
  def getEpoch(e: Int): SourceEpochNode
}

trait SourceEpochCollectionComponent extends ZkCollectionStateNodeComponent {
  this: ZkClientComponent with SourceEpochNodeComponent =>

  class SourceEpochCollectionImpl(parent: ZkNodeBase)
      extends ZkCollectionNodeImpl[SourceEpochNode, EpochCollection](
        "epochs",
        parent,
        (n, p) => new SourceEpochNodeImpl(n.toInt, p))
      with SourceEpochCollection {

    /**
      * Retrieves the latest known completed checkpoint for this source.
      * returns Long.minvalue if the source has no checkpoints.
      * If the subject is still active, this method might return different values upon each call
      *
      * @return
      */
    override def getLatestEpochId(): Future[Long] = async {
      await(getData()).map(o => o.latestEpoch).getOrElse(Long.MinValue)
    }

    /**
      * Retrieve the epochNode of the given epcoh
      *
      * @param e
      * @return
      */
    override def getEpoch(e: Int): SourceEpochNode = new SourceEpochNodeImpl(e, this)
  }

}
