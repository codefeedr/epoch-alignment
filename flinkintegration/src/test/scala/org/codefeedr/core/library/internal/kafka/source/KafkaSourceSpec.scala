package org.codefeedr.core.library.internal.kafka.source

import org.apache.flink.api.common.state.{ListState, OperatorStateStore}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.apache.flink.types.Row
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.codefeedr.configuration.KafkaConfiguration
import org.codefeedr.core.MockedLibraryServices
import org.codefeedr.core.library.metastore._
import org.codefeedr.core.library.metastore.sourcecommand.{KafkaSourceCommand, SourceCommand}
import org.codefeedr.model.zookeeper.Partition
import org.codefeedr.model.{RecordProperty, RecordSourceTrail, SubjectType, TrailedRecord}
import org.codefeedr.util.MockitoExtensions
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.scalatest.concurrent.{ConductorFixture, Conductors}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterEach}

import scala.async.Async.{async, await}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import org.codefeedr.util.MockitoExtensions._

class KafkaSourceSpec extends AsyncFlatSpec with MockitoSugar with BeforeAndAfterEach with Conductors with MockedLibraryServices with MockitoExtensions {

  private var subjectNode: SubjectNode = _
  private var jobNode: JobNode = _
  private var ctx: SourceFunction.SourceContext[SampleObject] = _
  private var closePromise: Promise[Unit] = _
  private var sampleObject: SampleObject = _
  private var manager: KafkaSourceManager = _

  private var consumer: KafkaSourceConsumer[SampleObject,SampleObject,Object] with KafkaSourceMapper[SampleObject,SampleObject,Object] = _

  private var initCtx : FunctionInitializationContext  = _
  private var context: FunctionSnapshotContext = _
  private var operatorStore: OperatorStateStore = _
  private var listState:ListState[KafkaSourceStateContainer] = _

  private var runtimeContext:StreamingRuntimeContext = _
  private var consumerFactory:KafkaConsumerFactory = _

  private var cpLock:Object = _

  private var thread:Thread = _


  override def beforeEach(): Unit = {
    super.beforeEach()
    subjectNode = mock[SubjectNode]
    jobNode = mock[JobNode]
    ctx = mock[SourceFunction.SourceContext[SampleObject]]
    sampleObject = new SampleObject()
    manager = mock[KafkaSourceManager]

    consumer = mock[KafkaSourceConsumer[SampleObject,SampleObject,Object] with KafkaSourceMapper[SampleObject,SampleObject,Object]]

    initCtx = mock[FunctionInitializationContext]
    context = mock[FunctionSnapshotContext]
    operatorStore = mock[OperatorStateStore]
    listState = mock[ListState[KafkaSourceStateContainer]]

    runtimeContext = mock[StreamingRuntimeContext]
    consumerFactory = mock[KafkaConsumerFactory]

    cpLock = new Object()

    when(subjectNode.getDataSync()) thenReturn
      Some(SubjectType("subjectuuid", "SampleObject",persistent = false, new Array[RecordProperty[_]](0)))

    closePromise = Promise[Unit]()
    when(manager.cancel) thenReturn closePromise.future


    when(initCtx.getOperatorStateStore) thenReturn operatorStore
    when(operatorStore.getListState[KafkaSourceStateContainer](ArgumentMatchers.any())) thenReturn listState
    when(listState.get()) thenReturn List.empty[KafkaSourceStateContainer].asJava

    when(consumer.getCurrentOffsets) thenReturn Map[Int,Long]()

    when(runtimeContext.isCheckpointingEnabled) thenReturn true

    when(ctx.getCheckpointLock).thenReturn (cpLock,cpLock)
    when(consumerFactory.create[RecordSourceTrail, Row](ArgumentMatchers.any[String](), ArgumentMatchers.any[Boolean]())(ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn (mock[KafkaConsumer[RecordSourceTrail,Row]])

    //Some default values
    when(manager.getLatestSubjectEpoch) thenReturn Future.successful(0L)
    when(manager.getEpochOffsets(ArgumentMatchers.any())) thenReturn Future.successful(Iterable.empty[Partition])
    when(manager.isCatchedUp(ArgumentMatchers.any())) thenReturn Future.successful(false)
  }


  "KafkaSource.InitRun" should "Call initializeRun on the manager" in {
    //Arrange
    val testKafkaSource = constructSource()

    //Act
    testKafkaSource.setRuntimeContext(runtimeContext)
    testKafkaSource.initializeState(initCtx)
    testKafkaSource.initRun()

    //Assert
    verify(manager, times(1)).initializeRun()
    assert(testKafkaSource.running)
  }

  it should "store the current offsets when snapshotState is called" in async {
    //Arrange
    val testKafkaSource = constructInitializedSource()
    val p = Promise[Unit]()
    when(consumer.getCurrentOffsets).thenReturn(Map(2 -> 10L))


    //Act
    testKafkaSource.doCycle(ctx)
    testKafkaSource.snapshotState(context)

    //Assert
    assert(testKafkaSource.checkpointOffsets(0)(2) == 10L)
    assert(testKafkaSource.running)
  }



  it should "store the offsets when snapshotState is called" in async {
    //Arrange
    val testKafkaSource = constructInitializedSource()
    when(consumer.getCurrentOffsets).thenReturn(Map(2 -> 10L))


    val context = mock[FunctionSnapshotContext]
    when(context.getCheckpointId) thenReturn 1

    //Act
    testKafkaSource.doCycle(ctx)
    testKafkaSource.snapshotState(context)

    //Assert
    assert(testKafkaSource.checkpointOffsets(1)(2) == 10L)
    assert(testKafkaSource.running)
  }

  it should "update list state stored for a checkpoint when snapshot state is called" in async {
    //Arrange
    val testKafkaSource = constructInitializedSource()
    when(consumer.getCurrentOffsets).thenReturn(Map(2 -> 10L))
    val context = mock[FunctionSnapshotContext]
    when(context.getCheckpointId) thenReturn 1

    //Act
    testKafkaSource.doCycle(ctx)
    testKafkaSource.snapshotState(context)

    //Assert
    verify(listState, times(1)).update(List(KafkaSourceStateContainer("0",Map(2->10L))).asJava)
    assert(testKafkaSource.running)
  }

  it should "commit offsets when notifyCheckpointComplete is called" in async {
    //Arrange
    val testKafkaSource = constructInitializedSource()
    when(consumer.getCurrentOffsets).thenReturn(Map(2 -> 10L))
    val context = mock[FunctionSnapshotContext]
    when(context.getCheckpointId) thenReturn 1

    //Act
    testKafkaSource.doCycle(ctx)
    testKafkaSource.snapshotState(context)
    testKafkaSource.notifyCheckpointComplete(1)

    //Assert
    verify(consumer, times(1)).commit(Map(2->10L))
    assert(testKafkaSource.running)
  }

  it should "mark the latest epoch and offsets when cancel is called" in async {
    //Arrange
    val testKafkaSource = constructInitializedSource()
    when(manager.getLatestSubjectEpoch) thenReturn Future.successful(1337L)
    when(manager.getEpochOffsets(1337L)) thenReturn Future.successful(Iterable(Partition(2,1338)))

    //Act
    await(testKafkaSource.cancelAsync())

    //Assert
    assert(testKafkaSource.finalSourceEpoch == 1337)
    assert(testKafkaSource.finalSourceEpochOffsets.exists(o => o.nr == 2 && o.offset == 1338))
  }


  it should "Close the source when a poll obtained all data of the final offsets" in async {
    //Arrange
    when(consumer.getCurrentOffsets).thenReturn(Map(3 -> 1339L))
    when(manager.getVerifiedFinalCheckpoint) thenReturn Some(2L)
    val context = mock[FunctionSnapshotContext]
    when(context.getCheckpointId) thenReturn 2
    when(manager.getLatestSubjectEpoch) thenReturn Future.successful(1L)
    when(consumer.higherOrEqual(Map(3 -> 1339L))).thenReturn(Some(true))
    when(manager.getEpochOffsets(1L)) thenReturn Future.successful(Iterable(Partition(3,1339)))
    val testKafkaSource = constructInitializedSource()

    //Act
    await(testKafkaSource.cancelAsync())
    testKafkaSource.snapshotState(context)
    val before = testKafkaSource.running
    testKafkaSource.doCycle(ctx)
    testKafkaSource.notifyCheckpointComplete(2)
    val after =testKafkaSource.running


    //Assert
    assert(before)
    assert(!after)
    assert(testKafkaSource.checkpointOffsets(2)(3) == 1339)
  }

  it should "not close if the offsets had not been reached" in async {
    //Arrange
    val testKafkaSource = constructInitializedSource()
    val epochCollectionNodeMock = mock[EpochCollectionNode]
    when(consumer.getCurrentOffsets).thenReturn(Map(3 -> 1338L))

    //Initialize with empty collection
    testKafkaSource.setRuntimeContext(runtimeContext)
    testKafkaSource.initializeState(initCtx)


    val context = mock[FunctionSnapshotContext]
    when(context.getCheckpointId) thenReturn 1
    when(subjectNode.getEpochs()) thenReturn epochCollectionNodeMock
    when(epochCollectionNodeMock.getLatestEpochId) thenReturn Future.successful(1L)
    when(manager.getEpochOffsets(1L)) thenReturn Future.successful(Iterable(Partition(3,1339)))

    //Act
    await(testKafkaSource.cancelAsync())
    testKafkaSource.snapshotState(context)
    val before = testKafkaSource.running
    testKafkaSource.doCycle(ctx)
    testKafkaSource.notifyCheckpointComplete(1)
    val after =testKafkaSource.running


    //Assert
    assert(before)
    assert(after)
    assert(testKafkaSource.checkpointOffsets(1)(3) == 1338)
  }

  it should "Swich to catchingUp state when receiving prepareSynchronize command on the next epoch" in async {
    //Arrange
    val testKafkaSource = constructInitializedSource()
    val context = getMockedContext(13)

    //Act
    testKafkaSource.apply(SourceCommand(KafkaSourceCommand.catchUp,None))
    testKafkaSource.snapshotState(context)

    //Assert
    verify(manager,times(1)).startedCatchingUp()
    assert(testKafkaSource.getState == KafkaSourceState.CatchingUp)
  }

  it should "not switch until snapshotstate is called" in async {
    //Arrange
    val testKafkaSource = constructSource()
    val context = getMockedContext(13)

    //Act
    testKafkaSource.apply(SourceCommand(KafkaSourceCommand.catchUp,None))

    //Assert
    verify(manager,times(0)).startedCatchingUp()
    assert(testKafkaSource.getState == KafkaSourceState.UnSynchronized)
  }

  it should "switch to the ready state when catched up" in async {
    //Arrange
    val testKafkaSource = constructSourceCatchingUp()
    val context = mock[FunctionSnapshotContext]
    when(context.getCheckpointId) thenReturn 1337L
    when(manager.isCatchedUp(ArgumentMatchers.any())) thenReturn Future.successful(true)

    //Act
    testKafkaSource.snapshotState(context)

    //Assert
    verify(manager, times(1)).notifyCatchedUp()
    verify(manager, times(1)).notifyStartedOnEpoch(1337L)
    assert(testKafkaSource.getState == KafkaSourceState.Ready)
  }

  it should "Notify the manager of starting on offsets when in ready state" in async {
    //Arrange
    val testKafkaSource = constructSourceReady()
    val context = mock[FunctionSnapshotContext]
    when(context.getCheckpointId) thenReturn 1337L
    when(manager.notifyStartedOnEpoch(ArgumentMatchers.any())) thenReturn Future.successful(())

    //Act
    testKafkaSource.snapshotState(context)

    //Assert
    verify(manager, times(1)).notifyStartedOnEpoch(1337L)
    assert(true)
  }

  it should "Switch to synchronized state when it reaches the synchronization epoch" in async {
    //Arrange
    val testKafkaSource = constructSourceReady()
    val context = getMockedContext(1337)
    when(manager.notifyStartedOnEpoch(ArgumentMatchers.any())) thenReturn Future.successful(())
    testKafkaSource.apply(SourceCommand(KafkaSourceCommand.synchronize, Some("1337")))

    //Act
    testKafkaSource.snapshotState(context)

    //Assert
    verify(manager, times(1)).notifySynchronized()
    assert(testKafkaSource.getState == KafkaSourceState.Synchronized)
  }

  it should "Switch not to synchronized state when it reaches some other epoch" in async {
    //Arrange
    val testKafkaSource = constructSourceReady()
    val context = getMockedContext(1337)
    when(manager.notifyStartedOnEpoch(ArgumentMatchers.any())) thenReturn Future.successful(())
    testKafkaSource.apply(SourceCommand(KafkaSourceCommand.synchronize, Some("1338")))

    //Act
    testKafkaSource.snapshotState(context)

    //Assert
    verify(manager, times(0)).notifySynchronized()
    assert(testKafkaSource.getState == KafkaSourceState.Ready)
  }

  it should "request offsets for a specific epoch when in synchronized mode" in async {
    //Arrange
    val testKafkaSource = constructSourceSynchronized()
    val context = mock[FunctionSnapshotContext]
    when(context.getCheckpointId) thenReturn 1337L
    when(manager.notifyStartedOnEpoch(ArgumentMatchers.any())) thenReturn Future.successful(())
    when(manager.nextSourceEpoch(1337L)) thenReturn Future.successful(Iterable(Partition(1,10)))

    //Act
    testKafkaSource.snapshotState(context)

    //Assert
    verify(manager, times(1)).notifyStartedOnEpoch(1337L)
    verify(manager, times(1)).nextSourceEpoch(1337L)
    assert(testKafkaSource.alignmentOffsets(1) == 10L)
  }


  "abort" should "do nothing when unsynchronized" in {
    //Arrange
    val source = constructInitializedSource()
    val command = SourceCommand(KafkaSourceCommand.abort, None)
    val ctx = getMockedContext(100)

    //act
    source.apply(command)
    val before = source.getState
    source.snapshotState(ctx)
    val after = source.getState

    //Assert
    verify(manager, times(0)).notifyAbort()
    assert(before == KafkaSourceState.UnSynchronized)
    assert(after == KafkaSourceState.UnSynchronized)
  }

  it should "transition the source back to unsynchronized when catching up" in async {
    //Arrange
    val source = constructSourceCatchingUp()
    val command = SourceCommand(KafkaSourceCommand.abort, None)
    val ctx = getMockedContext(100)

    //act
    source.apply(command)
    val before = source.getState
    source.snapshotState(ctx)
    val after = source.getState

    //Assert
    verify(manager, times(1)).notifyAbort()
    assert(before == KafkaSourceState.CatchingUp)
    assert(after == KafkaSourceState.UnSynchronized)
  }

  it should "transition the source back to unsynchronized when ready" in async {
    //Arrange
    val source = constructSourceReady()
    val command = SourceCommand(KafkaSourceCommand.abort, None)
    val ctx = getMockedContext(100)

    //act
    source.apply(command)
    val before = source.getState
    source.snapshotState(ctx)
    val after = source.getState

    //Assert
    verify(manager, times(1)).notifyAbort()
    assert(before == KafkaSourceState.Ready)
    assert(after == KafkaSourceState.UnSynchronized)
  }

  it should "transition the source back to unsynchronized when synchronized" in async {
    //Arrange
    val source = constructSourceSynchronized()
    val command = SourceCommand(KafkaSourceCommand.abort, None)
    val ctx = getMockedContext(100)

    //act
    source.apply(command)
    val before = source.getState
    source.snapshotState(ctx)
    val after = source.getState

    //Assert
    verify(manager, times(1)).notifyAbort()
    assert(before == KafkaSourceState.Synchronized)
    assert(after == KafkaSourceState.UnSynchronized)
  }

  /**
    * Obtains a mocked context for the given epoch, and also mocks the manager nextSourceEpoch for the given epoch with an empty partition collection
    * @param epoch the epoch to get a mocked context for
    */
  def getMockedContext(epoch: Long): FunctionSnapshotContext = {
    val context = mock[FunctionSnapshotContext]
    when(context.getCheckpointId) thenReturn epoch
    when(manager.nextSourceEpoch(epoch)) thenReturn Future.successful(Iterable.empty[Partition])
    when(manager.notifyStartedOnEpoch(epoch)) thenReturn Future.successful(())
    context
  }

  /**
    * Construct a kafkasource in the Synchronized state
    * Used an epoch: -100
    * @return
    */
  def constructSourceSynchronized(): TestKafkaSource = {
    //Arrange
    val source = constructSourceReady()
    val context = mock[FunctionSnapshotContext]
    when(context.getCheckpointId) thenReturn -100
    when(manager.notifyStartedOnEpoch(ArgumentMatchers.any())) thenReturn Future.successful(())
    when(manager.nextSourceEpoch(-100)) thenReturn Future.successful(Iterable.empty[Partition])


    source.apply(SourceCommand(KafkaSourceCommand.synchronize, Some("-100")))
    source.snapshotState(context)
    source
  }


  /**
    * Constructs a kafkaSource in the ready state
    * Used epoch: -100 and -200
    */
  def constructSourceReady(): TestKafkaSource = {
    val source = constructSourceCatchingUp()
    val context = getMockedContext(-100)
    when(manager.isCatchedUp(ArgumentMatchers.any())) thenReturn Future.successful(true)
    source.snapshotState(context)
    source
  }

  /**
    * Constructs a kafkaSource in the catching up state
    * Used epoch -200
    * @return
    */
  def constructSourceCatchingUp(): TestKafkaSource = {
    val source = constructInitializedSource()
    val context = getMockedContext(-200)
    source.apply(SourceCommand(KafkaSourceCommand.catchUp,None))
    source.snapshotState(context)
    source
  }

  def constructInitializedSource(): TestKafkaSource = {
    val source = constructSource()
    source.initializeState(initCtx)
    source.initRun()
    source
  }

  def constructSource(): TestKafkaSource = {
    val source = new TestKafkaSource(subjectNode,jobNode,kafkaConfiguration,consumerFactory,consumer)
    source.setRuntimeContext(runtimeContext)
    //Override the default manager
    source.manager = manager
    source
  }
}



class SampleObject {

}
import org.codefeedr.util.NoEventTime._

class TestKafkaSource(node: SubjectNode,jobNode: JobNode,kafkaConfiguration: KafkaConfiguration,kafkaConsumerFactory: KafkaConsumerFactory, mockedConsumer:KafkaSourceConsumer[SampleObject,SampleObject,Object]with KafkaSourceMapper[SampleObject,SampleObject,Object])
  extends KafkaSource[SampleObject,SampleObject,Object](node,jobNode,kafkaConfiguration,kafkaConsumerFactory) {

  override val sourceUuid: String = "testuuid"



  @transient override lazy val consumer: KafkaSourceConsumer[SampleObject, SampleObject, Object] with KafkaSourceMapper[SampleObject,SampleObject,Object] = mockedConsumer

  /**
    * Get typeinformation of the returned type
    *
    * @return
    */
  override def getProducedType: TypeInformation[SampleObject] = null

  override def transform(value: SampleObject, key: Object): SampleObject = value
}


object TestMapper extends KafkaSourceMapper[SampleObject,SampleObject,Object] {
  override def transform(value: SampleObject, key: Object): SampleObject = value
}



