package org.codefeedr.core.library.internal.kafka.sink

import org.codefeedr.model.zookeeper.Partition

import scala.async.Async.{async, await}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

trait EpochStateManagerComponent {

  val epochStateManager: EpochStateManager


}

/**
  * Managing the zookeeper state of an epochstate of a sink
  */
class EpochStateManager extends Serializable {

  /**
    * Precommits the current epoch state
    * Creates the relevant nodes in zookeeper
    */
  def preCommit(epochState: EpochState): Future[Unit] = async {
    await(guaranteeEpochNode(epochState))

    //Create all partition offsets of the current transaction
    await(
      Future.sequence(
        epochState.transactionState.offsetMap.map(a => {
          val partition = a._1
          val offset = a._2
          epochState.epochNode
            .getPartitions()
            .getChild(partition.toString)
            .create(Partition(partition, offset))
        })
      )
    )
  }

  /**
    * Perform the actual commit
    * Flags the node as committed
    */
  def commit(epochState: EpochState): Future[Unit] = async {
    //Perform await to convert return type to unit
    await(
      Future.sequence(
        epochState.transactionState.offsetMap.map(a => {
          val partition = a._1
          epochState.epochNode.getPartitions().getChild(partition.toString).setState(true)
        })
      ))
  }

  /**
    * Creates the epochnode if it does not exist yet
    * @return
    */
  private def guaranteeEpochNode(epochState: EpochState): Future[Unit] = async {
    if (!await(epochState.epochNode.exists())) {
      await(epochState.epochCollectionNode.asyncWriteLock(() => createEpochNode(epochState)))
    }
  }

  /**
    * Creates the epochNode and its dependencies
    * Should be called from within a writelock on the epochCollectionNode
    * @return
    */
  private def createEpochNode(epochState: EpochState): Future[Unit] = async {
    if (!await(epochState.epochNode.exists())) {
      await(epochState.epochNode.create())
      await(epochState.epochNode.getPartitions().create())
    }
  }

}