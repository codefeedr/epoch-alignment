package org.codefeedr.core.library.internal.kafka.source

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.core.library.metastore.{SubjectNode, SynchronizationState}
import org.codefeedr.model.zookeeper.{Consumer, Partition, QuerySource}

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, Future, blocking}

/**
  * Class responsible for observing and modifying zookeeper state and notifying the kafkasource of events/state changes
  * @param kafkaSource the kafkaSource this class manages
  */
class KafkaSourceManager(kafkaSource: GenericKafkaSource,
                         subjectNode: SubjectNode,
                         sourceUuid: String,
                         instanceUuid: String)
    extends LazyLogging {

  private val sourceNode = subjectNode.getSources().getChild(sourceUuid)
  private val consumerNode = sourceNode.getConsumers().getChild(instanceUuid)
  private val syncStateNode = sourceNode.getSyncState()

  lazy val cancel: Future[Unit] = subjectNode.awaitClose()

  /**
    * Called from the kafkaSource when a run is initialized
    */
  def initializeRun(): Unit = {
    //Create self on zookeeper
    val initialConsumer = Consumer(instanceUuid, null, System.currentTimeMillis())

    blocking {
      //Update zookeeper state blocking, because the source cannot start until the proper zookeeper state has been configured
      Await.ready(sourceNode.create(QuerySource(sourceUuid)), Duration(5, SECONDS))
      Await.ready(consumerNode.create(initialConsumer), Duration(5, SECONDS))
    }
  }

  def finalizeRun(): Unit = {
    //Finally unsubscribe from the library
    blocking {
      Await.ready(consumerNode.setState(false), Duration(5, SECONDS))
    }
  }

  def startedCatchingUp(): Future[Unit] = {
    syncStateNode.setData(SynchronizationState(1))
  }

  /**
    * Obtains offsets for the subscribed subject of the given epoch
    * If -1 is passed, obtains the current latest offsets
    */
  def getEpochOffsets(epoch: Long): Future[Iterable[Partition]] = async {
    logger.debug(s"Obtaining offsets for epoch $epoch")
    if (epoch == -1) {
      throw new Exception(
        s"Attempting to obtain endoffsets for epoch -1 in source of ${subjectNode.name}. Did you run the job with checkpointing enabled?")
    } else {
      await(subjectNode.getEpochs().getChild(epoch).getData()).get.partitions
    }
  }
}
