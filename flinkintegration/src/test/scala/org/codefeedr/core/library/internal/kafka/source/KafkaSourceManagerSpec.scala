package org.codefeedr.core.library.internal.kafka.source

import org.codefeedr.core.MockedLibraryServices
import org.codefeedr.core.library.metastore._
import org.codefeedr.model.zookeeper.{EpochCollection, Partition}
import org.codefeedr.util.MockitoExtensions
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterEach}

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

class KafkaSourceManagerSpec  extends AsyncFlatSpec with MockitoSugar with BeforeAndAfterEach with MockedLibraryServices with MockitoExtensions {

  private var subjectNode: SubjectNode = _

  private var sourceCollectionNode : QuerySourceCollection= _
  private var sourceNode: QuerySourceNode = _
  private var sourceSyncStateNode: SourceSynchronizationStateNode = _
  private var sourceEpochCollectionNode: SourceEpochCollection = _

  private var consumerCollection: ConsumerCollection = _
  private var consumerNode: ConsumerNode = _
  private var consumerSyncState: SourceSynchronizationStateNode = _

  private var otherConsumerNode: ConsumerNode = _
  private var otherConsumerSyncState:SourceSynchronizationStateNode = _

  private var epochCollection: EpochCollectionNode = _

  private var source: KafkaSource[String] = _

  private var completePromise = Promise[Unit]()

  override def beforeEach(): Unit = {

    subjectNode = mock[SubjectNode]

    sourceCollectionNode = mock[QuerySourceCollection]
    sourceNode = mock[QuerySourceNode]
    sourceSyncStateNode = mock[SourceSynchronizationStateNode]
    sourceEpochCollectionNode = mock[SourceEpochCollection]

    consumerCollection = mock[ConsumerCollection]
    consumerNode = mock[ConsumerNode]
    consumerSyncState = mock[SourceSynchronizationStateNode]

    otherConsumerNode = mock[ConsumerNode]
    otherConsumerSyncState = mock[SourceSynchronizationStateNode]

    epochCollection = mock[EpochCollectionNode]

    source = mock[KafkaSource[String]]

    when(subjectNode.getSources()) thenReturn sourceCollectionNode
    when(subjectNode.awaitClose()) thenReturn completePromise.future
    when(sourceCollectionNode.getChild(ArgumentMatchers.any[String]())) thenReturn sourceNode
    when(sourceNode.createSync(ArgumentMatchers.any())) thenReturn null
    when(sourceNode.getSyncState()) thenReturn sourceSyncStateNode
    when(sourceNode.getEpochs()) thenReturn sourceEpochCollectionNode
    mockLock(sourceEpochCollectionNode)
    mockLock(sourceNode)

    when(sourceNode.getConsumers()) thenReturn consumerCollection
    when(consumerCollection.getChild(ArgumentMatchers.any[String]())) thenReturn consumerNode
    when(consumerCollection.getChildrenSync()) thenReturn Iterable(consumerNode,otherConsumerNode)
    when(consumerNode.createSync(ArgumentMatchers.any())) thenReturn null
    when(consumerNode.getSyncState()) thenReturn consumerSyncState

    when(otherConsumerNode.getSyncState()) thenReturn otherConsumerSyncState

    when(subjectNode.getEpochs()) thenReturn epochCollection
  }

  def constructManager(): KafkaSourceManager = new KafkaSourceManager(source,subjectNode,"sourceuuid", "instanceuuid")

  "KafkaSourceManager.InitalizeRun" should "construct source and consumer if needed" in {
    //Arrange
    val manager = constructManager()

    //Act
    manager.initializeRun()

    //Assert
    verify(sourceNode, times(1)).createSync(ArgumentMatchers.any())
    verify(consumerNode, times(1)).createSync(ArgumentMatchers.any())
    assert(true)
  }


  "KafkaSourceManager" should "invoke cancel on a kafkaSource when a subject completes" in async {
    //Arrange
    val manager = constructManager()
    //Manager is the class passing events to the kafka source
    manager.initializeRun()

    //Act
    completePromise.success(())

    //Assert
    await(manager.cancel)
    assert(manager.cancel.isCompleted)
  }

  "KafkaSourceManager.startedCatchingUp" should "set the consumer to catchingUp" in {
    //Arrange
    val manager = constructManager()

    //Act
    manager.startedCatchingUp()

    //Assert
    verify(consumerSyncState, times(1)).setDataSync(SynchronizationState(KafkaSourceState.CatchingUp))
    assert(true)
  }


  "KafkaSourceManager.isCatchedUp" should "Return true if the passed offsets are all past the second-last epoch" in {
    //Arrange
    val manager = constructManager()
    when(epochCollection.getLatestEpochIdSync) thenReturn 3L
    val epoch = mock[EpochNode]
    when(epochCollection.getChild(2)) thenReturn epoch
    when(epoch.getDataSync()) thenReturn Some(Epoch(2,Seq(Partition(1,2L),Partition(2,2L),Partition(3,3L))))

    val comparison = Map(1-> 2L, 2->2L)

    //Act
    val r = manager.isCatchedUpSync(comparison)

    //Assert
    assert(r)
  }

  it should "Return true if the pre-last epoch does not exist" in {
    //Arrange
    val manager = constructManager()
    when(epochCollection.getLatestEpochIdSync) thenReturn 0L

    val comparison = Map(1-> 2L, 2->2L)

    //Act
    val r = manager.isCatchedUpSync(comparison)

    //Assert
    assert(r)
  }

  it should "Return false if for some partition the offset is not past the pre-last epoch" in {
    //Arrange
    val manager = constructManager()
    when(epochCollection.getLatestEpochIdSync) thenReturn 3L
    val epoch = mock[EpochNode]
    when(epochCollection.getChild(2)) thenReturn epoch
    when(epoch.getDataSync()) thenReturn Some(Epoch(2,Seq(Partition(1,2L),Partition(2,3L),Partition(3,3L))))

    val comparison = Map(1-> 2L, 2->2L)

    //Act
    val r = manager.isCatchedUpSync(comparison)

    //Assert
    assert(!r)
  }


  "KafkaSourceManager.notifyCatchedUp" should "Set the state of the considered consumer to ready" in {
    //Arrange
    val manager = constructManager()
    when(sourceSyncStateNode.getDataSync()) thenReturn  Some(SynchronizationState(KafkaSourceState.CatchingUp))
    when(consumerSyncState.getDataSync()) thenReturn  Some(SynchronizationState(KafkaSourceState.CatchingUp))
    when(otherConsumerSyncState.getDataSync()) thenReturn Some(SynchronizationState(KafkaSourceState.CatchingUp))

    //Act
    manager.notifyCatchedUp()

    //Assert
    verify(consumerSyncState,times(1)).setDataSync(SynchronizationState(KafkaSourceState.Ready))
    assert(true)
  }

  it should "set the state of the source to ready if all consumers are ready" in {
    //Arrange
    val manager = constructManager()
    when(sourceSyncStateNode.getDataSync()) thenReturn  Some(SynchronizationState(KafkaSourceState.CatchingUp))
    when(consumerSyncState.getDataSync()) thenReturn  Some(SynchronizationState(KafkaSourceState.Ready))
    when(otherConsumerSyncState.getDataSync()) thenReturn Some(SynchronizationState(KafkaSourceState.Ready))

    //Act
    manager.notifyCatchedUp()

    //Assert
    verify(sourceSyncStateNode,times(1)).setData(SynchronizationState(KafkaSourceState.Ready))
    assert(true)
  }

  it should "not set the state of the source to ready if some consumer is not yet ready" in {
    //Arrange
    val manager = constructManager()
    when(sourceSyncStateNode.getDataSync()) thenReturn Some(SynchronizationState(KafkaSourceState.CatchingUp))
    when(consumerSyncState.getDataSync()) thenReturn Some(SynchronizationState(KafkaSourceState.Ready))
    when(otherConsumerSyncState.getDataSync()) thenReturn Some(SynchronizationState(KafkaSourceState.CatchingUp))

    //Act
    manager.notifyCatchedUp()

    //Assert
    verify(sourceSyncStateNode,times(0)).setData(SynchronizationState(KafkaSourceState.Ready))
    assert(true)
  }

  "KafkaSourceManager.notifySynchronized" should "Set the state of the considered consumer to synchronized" in {
    //Arrange
    val manager = constructManager()
    when(sourceSyncStateNode.getDataSync()) thenReturn Some(SynchronizationState(KafkaSourceState.Ready))
    when(consumerSyncState.getDataSync()) thenReturn Some(SynchronizationState(KafkaSourceState.Ready))
    when(otherConsumerSyncState.getDataSync()) thenReturn Some(SynchronizationState(KafkaSourceState.Ready))

    //Act
    manager.notifySynchronized()

    //Assert
    verify(consumerSyncState,times(1)).setDataSync(SynchronizationState(KafkaSourceState.Synchronized))
    assert(true)
  }

  it should "set the state of the source to synchronized if all consumers are synchronized" in {
    //Arrange
    val manager = constructManager()
    when(sourceSyncStateNode.getDataSync()) thenReturn Some(SynchronizationState(KafkaSourceState.Ready))
    when(consumerSyncState.getDataSync()) thenReturn Some(SynchronizationState(KafkaSourceState.Synchronized))
    when(otherConsumerSyncState.getDataSync()) thenReturn Some(SynchronizationState(KafkaSourceState.Synchronized))

    //Act
    manager.notifySynchronized()

    //Assert
    verify(sourceSyncStateNode,times(1)).setData(SynchronizationState(KafkaSourceState.Synchronized))
    assert(true)
  }

  it should "not set the state of the source to ready if some consumer is not yet ready" in {
    //Arrange
    val manager = constructManager()
    when(sourceSyncStateNode.getDataSync()) thenReturn  Some(SynchronizationState(KafkaSourceState.Ready))
    when(consumerSyncState.getDataSync()) thenReturn Some(SynchronizationState(KafkaSourceState.Synchronized))
    when(otherConsumerSyncState.getDataSync()) thenReturn Some(SynchronizationState(KafkaSourceState.Ready))

    //Act
    manager.notifySynchronized()

    //Assert
    verify(sourceSyncStateNode,times(0)).setData(SynchronizationState(KafkaSourceState.Synchronized))
    assert(true)
  }



  "NotifyStartedOnEpoch" should "update the latest epoch of the source" in {
    //Arrange
    val manager = constructManager()
    when(sourceEpochCollectionNode.getLatestEpochIdSync()) thenReturn 0L

    //Act
    manager.notifyStartedOnEpochSync(1)

    //Assert
    verify(sourceEpochCollectionNode, times(1)).setDataSync(EpochCollection(1))
    assert(true)
  }

  it should "do nothing if the latest epoch was alreadt set" in {
    //Arrange
    val manager = constructManager()
    when(sourceEpochCollectionNode.getLatestEpochIdSync()) thenReturn 1L

    //Act
    manager.notifyStartedOnEpochSync(1)

    //Assert
    verify(sourceEpochCollectionNode, times(0)).setDataSync(ArgumentMatchers.any())
    assert(true)
  }
}
