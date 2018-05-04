package org.codefeedr.core.library.internal.kafka.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.codefeedr.core.MockedLibraryServices
import org.codefeedr.core.library.metastore._
import org.codefeedr.util.MockitoExtensions
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterEach}
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.{Future, Promise}

class KafkaSourceManagerSpec  extends AsyncFlatSpec with MockitoSugar with BeforeAndAfterEach with MockedLibraryServices with MockitoExtensions {

  private var subjectNode: SubjectNode = _

  private var sourceCollectionNode : QuerySourceCollection= _
  private var sourceNode: QuerySourceNode = _

  private var consumerCollection: ConsumerCollection = _
  private var consumerNode: ConsumerNode = _

  private var source: KafkaSource[String] = _

  private var completePromise = Promise[Unit]()

  override def beforeEach(): Unit = {

    subjectNode = mock[SubjectNode]

    sourceCollectionNode = mock[QuerySourceCollection]
    sourceNode = mock[QuerySourceNode]

    consumerCollection = mock[ConsumerCollection]
    consumerNode = mock[ConsumerNode]

    source = mock[KafkaSource[String]]

    when(subjectNode.getSources()) thenReturn sourceCollectionNode
    when(subjectNode.awaitClose()) thenReturn completePromise.future
    when(sourceCollectionNode.getChild(ArgumentMatchers.any[String]())) thenReturn sourceNode
    when(sourceNode.create(ArgumentMatchers.any())) thenReturn Future.successful(null)

    when(sourceNode.getConsumers()) thenReturn consumerCollection
    when(consumerCollection.getChild(ArgumentMatchers.any[String]())) thenReturn consumerNode
    when(consumerNode.create(ArgumentMatchers.any())) thenReturn Future.successful(null)
  }

  "KafkaSourceManager.InitalizeRun" should "construct source and consumer if needed" in {
    //Arrange
    val manager = new KafkaSourceManager(source,subjectNode,"sourceuuid", "instanceuuid")

    //Act
    manager.initializeRun()

    //Assert
    verify(sourceNode, times(1)).create(ArgumentMatchers.any())
    verify(consumerNode, times(1)).create(ArgumentMatchers.any())
    assert(true)
  }
}
