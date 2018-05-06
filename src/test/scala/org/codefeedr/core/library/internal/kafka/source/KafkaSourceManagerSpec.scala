package org.codefeedr.core.library.internal.kafka.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.codefeedr.core.MockedLibraryServices
import org.codefeedr.core.library.metastore._
import org.codefeedr.model.zookeeper.Partition
import org.codefeedr.util.MockitoExtensions
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterEach, FlatSpec}
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.async.Async.{async, await}

class KafkaSourceManagerSpec  extends AsyncFlatSpec with MockitoSugar with BeforeAndAfterEach with MockedLibraryServices with MockitoExtensions {

  private var subjectNode: SubjectNode = _

  private var sourceCollectionNode : QuerySourceCollection= _
  private var sourceNode: QuerySourceNode = _
  private var sourceSyncStateNode: SourceSynchronizationStateNode = _

  private var consumerCollection: ConsumerCollection = _
  private var consumerNode: ConsumerNode = _

  private var epochCollection: EpochCollectionNode = _

  private var source: KafkaSource[String] = _

  private var completePromise = Promise[Unit]()

  override def beforeEach(): Unit = {

    subjectNode = mock[SubjectNode]

    sourceCollectionNode = mock[QuerySourceCollection]
    sourceNode = mock[QuerySourceNode]
    sourceSyncStateNode = mock[SourceSynchronizationStateNode]

    consumerCollection = mock[ConsumerCollection]
    consumerNode = mock[ConsumerNode]

    epochCollection = mock[EpochCollectionNode]

    source = mock[KafkaSource[String]]

    when(subjectNode.getSources()) thenReturn sourceCollectionNode
    when(subjectNode.awaitClose()) thenReturn completePromise.future
    when(sourceCollectionNode.getChild(ArgumentMatchers.any[String]())) thenReturn sourceNode
    when(sourceNode.create(ArgumentMatchers.any())) thenReturn Future.successful(null)
    when(sourceNode.getSyncState()) thenReturn sourceSyncStateNode

    when(sourceNode.getConsumers()) thenReturn consumerCollection
    when(consumerCollection.getChild(ArgumentMatchers.any[String]())) thenReturn consumerNode
    when(consumerNode.create(ArgumentMatchers.any())) thenReturn Future.successful(null)

    when(subjectNode.getEpochs()) thenReturn epochCollection
  }

  def constructManager(): KafkaSourceManager = new KafkaSourceManager(source,subjectNode,"sourceuuid", "instanceuuid")

  "KafkaSourceManager.InitalizeRun" should "construct source and consumer if needed" in {
    //Arrange
    val manager = constructManager()

    //Act
    manager.initializeRun()

    //Assert
    verify(sourceNode, times(1)).create(ArgumentMatchers.any())
    verify(consumerNode, times(1)).create(ArgumentMatchers.any())
    assert(true)
  }


  "KafkaSourceManager" should "invoke cancel on a kafkaSource when a subject completes" in async {
    //Arrange
    val manager = constructManager()
    //Manager is the class passing events to the kafka source
    manager.initializeRun()

    //Act
    completePromise.success()

    //Assert
    await(manager.cancel)
    assert(manager.cancel.isCompleted)
  }

  "KafkaSourceManager.startedCatchingUp" should "set the sourceNode to catchingUp" in {
    //Arrange
    val manager = constructManager()

    //Act
    manager.startedCatchingUp()

    //Assert
    verify(sourceSyncStateNode, times(1)).setData(SynchronizationState(1))
    assert(true)
  }


  "KafkaSourceManager.isCatchedUp" should "Return true if the passed offsets are all past the second-last epoch" in async {
    //Arrange
    val manager = constructManager()
    when(epochCollection.getLatestEpochId()) thenReturn Future.successful(3L)
    val epoch = mock[EpochNode]
    when(epochCollection.getChild(2)) thenReturn epoch
    when(epoch.getData()) thenReturn Future.successful(Some(Epoch(2,Seq(Partition(1,2L),Partition(2,2L),Partition(3,3L)))))

    val comparison = Map(1-> 2L, 2->2L)

    //Act
    val r = await(manager.isCatchedUp(comparison))

    //Assert
    assert(r)
  }

  it should "Return true if the pre-last epoch does not exist" in async {
    //Arrange
    val manager = constructManager()
    when(epochCollection.getLatestEpochId()) thenReturn Future.successful(0L)

    val comparison = Map(1-> 2L, 2->2L)

    //Act
    val r = await(manager.isCatchedUp(comparison))

    //Assert
    assert(r)
  }

  it should "Return false if for some partition the offset is not past the pre-last epoch" in async {
    //Arrange
    val manager = constructManager()
    when(epochCollection.getLatestEpochId()) thenReturn Future.successful(3L)
    val epoch = mock[EpochNode]
    when(epochCollection.getChild(2)) thenReturn epoch
    when(epoch.getData()) thenReturn Future.successful(Some(Epoch(2,Seq(Partition(1,2L),Partition(2,3L),Partition(3,3L)))))

    val comparison = Map(1-> 2L, 2->2L)

    //Act
    val r = await(manager.isCatchedUp(comparison))

    //Assert
    assert(!r)
  }

}
