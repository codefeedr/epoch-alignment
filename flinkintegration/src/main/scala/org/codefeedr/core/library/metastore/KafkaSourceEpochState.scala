package org.codefeedr.core.library.metastore

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.core.library.internal.kafka.meta.SourceEpoch

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future



/**
  * Class used when creating a synchronized source
  * @param subjectNode node describing the subject the source is subscribed on
  * @param querySourceNode the sourceNode this state belongs to
  */
class KafkaSourceEpochState(subjectNode: SubjectNode, querySourceNode: QuerySourceNode)
    extends LazyLogging {

  //Collection of child epochs
  private lazy val sourceEpochCollection: SourceEpochCollection = querySourceNode.getEpochs()

  /**
    * Obtains the next sourceEpoch for the given checkpointId
    * @param checkpointId id the the checkpoint currently starting on
    */
  def nextSourceEpoch(checkpointId: Long): Future[SourceEpoch] = {
    //Perform all operations within a write lock
    sourceEpochCollection.asyncWriteLock(() =>
      async {
        val resultNode = sourceEpochCollection.getChild(s"$checkpointId")
        //If the node already exists, some other worker already performed the operation. Returns this information
        if (await(resultNode.exists())) {
          await(resultNode.getData().map(o => o.get))
          //If the previous checkpoint was already synchronized, we should continue on it
        } else if (await(sourceEpochCollection.getChild(s"${checkpointId - 1}").exists())) {
          await(createNextSourceEpoch(checkpointId))
          //Otherwise, we should start creating synchronized checkpoints form this point onward
        } else {
          await(createFirstSourceEpoch(checkpointId))
        }
    })
  }

  /**
    * Creates a sequent sourceEpoch node in zookeeper state, and returns its contents
    * @param checkpointId if of the checkpoint currently starting on
    * @return
    */
  private def createNextSourceEpoch(checkpointId: Long): Future[SourceEpoch] = async {
    val previous =
      await(sourceEpochCollection.getChild(s"${checkpointId - 1}").getData()).get.subjectEpochId
    val sourceEpoch = await(CreateNextSourceEpoch(previous, checkpointId))
    await(sourceEpochCollection.getChild(s"$checkpointId").create(sourceEpoch))
    sourceEpoch
  }

  /**
    * Creates a new sourceEpoch based on the information about the subject in zookeeper
    * @param previousSubjectEpoch id the the previous subject epoch
    * @param checkpointId id of the epoch of the current task
    * @return
    */
  private def CreateNextSourceEpoch(previousSubjectEpoch: Long,
                                    checkpointId: Long): Future[SourceEpoch] = async {
    val nextSourceEpoch = subjectNode.getEpochs().getChild(s"${previousSubjectEpoch + 1}")
    //If the next epoch is available, use it. Otherwise use the previous epoch (Which means this source will not be reading new data)
    if (await(nextSourceEpoch.exists())) {
      await(CreateSourceEpoch(nextSourceEpoch, checkpointId))
    } else {
      val previousSourceEpoch = subjectNode.getEpochs().getChild(s"$previousSubjectEpoch")
      logger.warn(
        s"Source on ${subjectNode.name} reusing subject epoch $previousSourceEpoch because no newer epochs were found for source epoch $checkpointId")
      await(CreateSourceEpoch(previousSourceEpoch, checkpointId))
    }
  }

  /**
    * Creates the first source epoch.
    * @param checkpointId id of the epoch of the current task
    * @return
    */
  private def createFirstSourceEpoch(checkpointId: Long): Future[SourceEpoch] = async {
    val maxEpoch = await(subjectNode.getEpochs().getLatestEpochId)
    val sourceEpoch =
      await(CreateSourceEpoch(subjectNode.getEpochs().getChild(s"$maxEpoch"), checkpointId))
    await(sourceEpochCollection.getChild(s"$checkpointId").create(sourceEpoch))
    sourceEpoch
  }

  /**
    * Creates a new SourceEpoch based on some Epoch
    * @param epochNode epoch (of the subject) to base the new sourceEpoch on
    * @return
    */
  private def CreateSourceEpoch(epochNode: EpochNode, checkpointId: Long): Future[SourceEpoch] =
    async {
      val partitions = await(epochNode.getPartitionData())
      SourceEpoch(partitions.toList, checkpointId, epochNode.getEpoch())
    }

}
