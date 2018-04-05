package org.codefeedr.core.library.internal.kafka.sink

import org.codefeedr.core.library.metastore.{EpochCollectionNode, EpochNode}
import org.codefeedr.model.zookeeper.Partition
import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}

import scala.concurrent.Future

class EpochState(transactionState: TransactionState, epochCollectionNode: EpochCollectionNode) {

  lazy val epochNode: EpochNode = epochCollectionNode.getChild(transactionState.checkPointId.toString)

  /**
    * Precommits the current epoch state
    * Creates the relevant nodes in zookeeper
    */
  def preCommit(): Future[Unit] = async {
    await(guaranteeEpochNode)

    //Create all partition offsets of the current transaction
    await(
      Future.sequence(
        transactionState.offsetMap.map(a => {
          val partition = a._1._2
          val offset= a._2
          epochNode.getPartitions().getChild(partition.toString).create(Partition(partition, offset))
        })
      )
    )
  }

  /**
  * Perform the actual commit
  * Flags the node as committed
  */
  def commit(): Future[Unit] = async {
    //Perform await to convert return type to unit
    await(Future.sequence(
      transactionState.offsetMap.map(a => {
        val partition = a._1._2
        epochNode.getPartitions().getChild(partition.toString).setState(true)
      })
    ))
  }


  /**
    * Creates the epochnode if it does not exist yet
    * @return
    */
  private def guaranteeEpochNode: Future[Unit] = async {
    if (!await(epochNode.exists())) {
      await(epochCollectionNode.asyncWriteLock(() => createEpochNode()))
    }
  }

  /**
    * Creates the epochNode and its dependencies
    * Should be called from within a writelock on the epochCollectionNode
    * @return
    */
  private def createEpochNode(): Future[Unit] = async {
    if (!await(epochNode.exists())) {
      await(epochNode.create())
      await(epochNode.getPartitions().create())
    }
  }

}
