package org.codefeedr.core.library.internal.kafka.source

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.core.library.metastore.{ConsumerNode, SubjectNode}
import org.codefeedr.model.zookeeper.{Consumer, QuerySource}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, blocking}
import scala.concurrent.duration.{Duration, SECONDS}

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

}
