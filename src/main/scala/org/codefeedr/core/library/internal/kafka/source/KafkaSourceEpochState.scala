package org.codefeedr.core.library.internal.kafka.source

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.core.library.internal.kafka.meta.{SourceEpoch, TopicPartitionOffsets}
import org.codefeedr.core.library.metastore._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}

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
    * @param checkpointId
    */
  def nextSourceEpoch(checkpointId: Int): Future[SourceEpoch] = {
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
    * @param checkpointId
    * @return
    */
  private def createNextSourceEpoch(checkpointId: Int): Future[SourceEpoch] = async {
    val previous =
      await(sourceEpochCollection.getChild(s"${checkpointId - 1}").getData()).get.subjectEpochId
    val sourceEpoch = await(CreateNextSourceEpoch(previous, checkpointId))
    await(sourceEpochCollection.getChild(s"$checkpointId").create(sourceEpoch))
    sourceEpoch
  }

  /**
    * Creates a new sourceEpoch based on the information about the subject in zookeeper
    * @param previousSubjectEpoch
    * @param checkpointId
    * @return
    */
  private def CreateNextSourceEpoch(previousSubjectEpoch: Int,
                                    checkpointId: Int): Future[SourceEpoch] = async {
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
    * @param checkpointId
    * @return
    */
  private def createFirstSourceEpoch(checkpointId: Int): Future[SourceEpoch] = async {
    val children = await(subjectNode.getEpochs().getChildren())
    val maxEpoch = children.map(o => o.getEpoch()).max
    val sourceEpoch =
      await(CreateSourceEpoch(subjectNode.getEpochs().getChild(s"$maxEpoch"), checkpointId))
    await(sourceEpochCollection.getChild(s"$checkpointId").create(sourceEpoch))
    sourceEpoch
  }

  /**
    * Creates a new SourceEpoch based on some Epoch
    * @param epochNode
    * @return
    */
  private def CreateSourceEpoch(epochNode: EpochNode, checkpointId: Int): Future[SourceEpoch] =
    async {
      val partitions = await(epochNode.getPartitionData())
      SourceEpoch(partitions.toList, checkpointId, epochNode.getEpoch())
    }

}
