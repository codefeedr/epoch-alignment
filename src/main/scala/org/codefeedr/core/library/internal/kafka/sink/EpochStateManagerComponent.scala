package org.codefeedr.core.library.internal.kafka.sink

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.core.library.metastore.EpochNode
import org.codefeedr.model.zookeeper.Partition
import org.codefeedr.util.Stopwatch

import scala.async.Async.{async, await}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

trait EpochStateManagerComponent {

  val epochStateManager: EpochStateManager

}

/**
  * Managing the zookeeper state of an epochstate of a sink
  */
class EpochStateManager extends Serializable with LazyLogging {

  /**
    * Precommits the current epoch state
    * Creates the relevant nodes in zookeeper
    */
  def preCommit(epochState: EpochState): Future[Unit] = async {
    val sw = Stopwatch.start()
    val epochNode = await(guaranteeEpochNode(epochState))
    await(epochNode.asyncWriteLock(() =>
      async {
        //Create all partition offsets of the current transaction

        logger.debug(
          s"precommitting epoch ${epochState.transactionState.checkPointId}: ${epochState.transactionState.offsetMap}")

        await(
          Future.sequence(
            epochState.transactionState.offsetMap.map(a =>
              async {
                val partition = a._1
                val offset = a._2
                val partitionNode = epochState.epochNode
                  .getPartitions()
                  .getChild(partition.toString)

                //If the node already exists (because some other worker created it), update it if its own offset is higher
                /*  if(await(partitionNode.exists()))
            {
            val oldOffset = await(partitionNode.getData()).get.offset
              if(offset > oldOffset) {
                await(partitionNode.setData(Partition(partition, offset)))
              }
              //Otherwise, create it with the new offset
            } else {*/
                await(partitionNode.create(Partition(partition, offset)))
                /*}*/
            })
          ))
    }))
    logger.debug(s"Precommit completed in ${sw.elapsed()} of epoch ${epochState.transactionState.checkPointId}: ${epochState.transactionState.offsetMap}")
  }

  /**
    * Perform the actual commit
    * Flags the node as committed
    */
  def commit(epochState: EpochState): Future[Unit] = async {
    val sw = Stopwatch.start()
    //Perform await to convert return type to unit
    await(
      Future.sequence(
        epochState.transactionState.offsetMap.map(a => {
          val partition = a._1
          epochState.epochNode.getPartitions().getChild(partition.toString).setState(true)
        })
      ))

    //TODO: Unit test this behavior
    if (await(epochState.epochNode.getPartitions().getState())) {
      logger.debug(s"Completing epoch ${epochState.epochNode.getEpoch()}")
      await(epochState.epochNode.setState(true))
      logger.debug(s"Completed epoch ${epochState.epochNode.getEpoch()}")
    }
    logger.debug(s"Commit completed in ${sw.elapsed()} of epoch ${epochState.transactionState.checkPointId}: ${epochState.transactionState.offsetMap}")
  }

  /**
    * Creates the epochnode if it does not exist yet
    * @return
    */
  private def guaranteeEpochNode(epochState: EpochState): Future[EpochNode] = async {
    if (!await(epochState.epochNode.exists())) {
      await(epochState.epochCollectionNode.asyncWriteLock(() => createEpochNode(epochState)))
    }
    epochState.epochNode
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
