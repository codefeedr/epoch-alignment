package org.codefeedr.core.library.internal.kafka.source

import org.apache.flink.api.common.state.{ListState, OperatorStateStore}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.apache.flink.types.Row
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.codefeedr.core.MockedLibraryServices
import org.codefeedr.core.library.metastore._
import org.codefeedr.core.library.metastore.sourcecommand.{KafkaSourceCommand, SourceCommand}
import org.codefeedr.model.zookeeper.Partition
import org.codefeedr.model.{RecordProperty, RecordSourceTrail, SubjectType, TrailedRecord}
import org.codefeedr.util.MockitoExtensions
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterEach}

import scala.async.Async.{async, await}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{Future, Promise}

class KafkaSourceSpec extends AsyncFlatSpec with MockitoSugar with BeforeAndAfterEach with MockedLibraryServices with MockitoExtensions {

  private var subjectNode: SubjectNode = _
  private var ctx: SourceFunction.SourceContext[SampleObject] = _
  private var closePromise: Promise[Unit] = _
  private var sampleObject: SampleObject = _
  private var manager: KafkaSourceManager = _

  private var consumer: KafkaSourceConsumer[SampleObject] = _

  private var initCtx : FunctionInitializationContext  = _
  private var operatorStore: OperatorStateStore = _
  private var listState:ListState[(Int, Long)] = _

  private var runtimeContext:StreamingRuntimeContext = _
  private var consumerFactory:KafkaConsumerFactory = _

  private var cpLock:Object = _

  private var thread:Thread = _


  override def beforeEach(): Unit = {
    subjectNode = mock[SubjectNode]
    ctx = mock[SourceFunction.SourceContext[SampleObject]]
    sampleObject = new SampleObject()
    manager = mock[KafkaSourceManager]

    consumer = mock[KafkaSourceConsumer[SampleObject]]

    initCtx = mock[FunctionInitializationContext]
    operatorStore = mock[OperatorStateStore]
    listState = mock[ListState[(Int, Long)]]

    runtimeContext = mock[StreamingRuntimeContext]
    consumerFactory = mock[KafkaConsumerFactory]

    cpLock = new Object()

    when(subjectNode.getDataSync()) thenReturn
      Some(SubjectType("subjectuuid", "SampleObject",persistent = false, new Array[RecordProperty[_]](0)))

    closePromise = Promise[Unit]()
    when(manager.cancel) thenReturn closePromise.future


    when(initCtx.getOperatorStateStore) thenReturn operatorStore
    when(operatorStore.getListState[(Int, Long)](ArgumentMatchers.any())) thenReturn listState
    when(listState.get()) thenReturn List[(Int, Long)]().asJava

    when(consumer.getCurrentOffsets) thenReturn mutable.Map[Int,Long]()
    when(consumer.poll(ArgumentMatchers.any())) thenReturn Map[Int, Long]()

    when(runtimeContext.isCheckpointingEnabled) thenReturn true

    when(ctx.getCheckpointLock).thenReturn (cpLock,cpLock)
    when(consumerFactory.create[RecordSourceTrail, Row](ArgumentMatchers.any[String]())(ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn (mock[KafkaConsumer[RecordSourceTrail,Row]])

    //Some default values
    when(manager.getLatestSubjectEpoch()) thenReturn Future.successful(0L)
    when(manager.getEpochOffsets(0L)) thenReturn Future.successful(Iterable.empty[Partition])
  }


  "KafkaSource.Run" should "Call initializeRun on the manager" in async {
    //Arrange
    val testKafkaSource = constructSource()

    //Act
    runAsync(testKafkaSource)
    while(!testKafkaSource.inititialized){}

    //Assert
    verify(manager, times(1)).initializeRun()

    assert(testKafkaSource.running)
  }

  it should "initialize with startoffsets from the consumer" in async {
    //Arrange
    val testKafkaSource = constructSource()
    when(consumer.getCurrentOffsets) thenReturn mutable.Map[Int, Long](1 -> 10,2->13)

    //Act
    runAsync(testKafkaSource)
    while(!testKafkaSource.inititialized){}

    //Assert
    verify(manager, times(1)).initializeRun()

    assert(testKafkaSource.currentOffsets(2) == 13L)
    assert(testKafkaSource.currentOffsets(1) == 10L)
    assert(testKafkaSource.running)
  }


  it should "store the current offsets when snapshotState is called" in async {
    //Arrange
    val testKafkaSource = constructSource()
    val p = Promise[Unit]()
    when(consumer.poll(ctx)).thenAnswer(awaitAndReturn(p, Map(2 -> 10L),2))

    //Act
    runAsync(testKafkaSource)
    await(p.future)

    //Assert
    assert(testKafkaSource.currentOffsets(2) == 10)
    assert(testKafkaSource.running)
  }

  it should "increment the offsets when multiple polls are perfomed" in async {
    //Arrange
    val testKafkaSource = constructSource()

    //Act
    val p = Promise[Unit]()
    when(consumer.poll(ctx)).thenAnswer(awaitAndReturn(p, Map(2 -> 10L),2))
    runAsync(testKafkaSource)
    await(p.future)

    val p2 = Promise[Unit]()
    when(consumer.poll(ctx)).thenAnswer(awaitAndReturn(p2, Map(2 -> 12L,1->2L),2))
    await(p2.future)

    //Assert
    assert(testKafkaSource.currentOffsets(2) == 12L)
    assert(testKafkaSource.currentOffsets(1) == 2L)
    assert(testKafkaSource.running)
  }


  it should "store the offsets when snapshotState is called" in async {
    //Arrange
    val testKafkaSource = constructSource()
    val p = Promise[Unit]()
    when(consumer.poll(ctx)).thenAnswer(awaitAndReturn(p, Map(2 -> 10L),2))

    val context = mock[FunctionSnapshotContext]
    when(context.getCheckpointId) thenReturn 1

    //Act
    runAsync(testKafkaSource)
    await(p.future)
    testKafkaSource.snapshotState(context)

    //Assert
    assert(testKafkaSource.checkpointOffsets(1)(2) == 10L)
    assert(testKafkaSource.running)
  }

  it should "commit offsets and update liststate stored for a checkpoint when notify complete is called" in async {
    //Arrange
    val testKafkaSource = constructSource()
    val p = Promise[Unit]()
    when(consumer.poll(ctx)).thenAnswer(awaitAndReturn(p, Map(2 -> 10L),2))
    val context = mock[FunctionSnapshotContext]
    when(context.getCheckpointId) thenReturn 1

    //Act
    runAsync(testKafkaSource)
    await(p.future)
    testKafkaSource.snapshotState(context)
    cpLock.synchronized {
      testKafkaSource.notifyCheckpointComplete(1)
    }

    //Assert
    verify(listState, times(1)).clear()
    verify(listState, times(1)).add((2,10L))
    verify(consumer, times(1)).commit(Map(2->10L))
    assert(testKafkaSource.running)
  }

  it should "mark the latest epoch and offsets when cancel is called" in async {
    //Arrange
    val testKafkaSource = constructSource()
    val epochCollectionNodeMock = mock[EpochCollectionNode]

    when(manager.getLatestSubjectEpoch()) thenReturn Future.successful(1337L)
    when(manager.getEpochOffsets(1337L)) thenReturn Future.successful(Iterable(Partition(2,1338)))

    //Act
    testKafkaSource.cancel()

    //Assert
    assert(testKafkaSource.finalSourceEpoch == 1337)
    assert(testKafkaSource.finalSourceEpochOffsets.exists(o => o.nr == 2 && o.offset == 1338))
  }


  it should "Close the source when a poll obtained all data of the final offsets" in async {
    //Arrange
    val testKafkaSource = constructSource()
    val epochCollectionNodeMock = mock[EpochCollectionNode]
    val p = Promise[Unit]()
    when(consumer.poll(ctx)).thenAnswer(awaitAndReturn(p, Map(3 -> 1339L),2))

    val context = mock[FunctionSnapshotContext]
    when(context.getCheckpointId) thenReturn 2


    when(manager.getLatestSubjectEpoch()) thenReturn Future.successful(1L)
    when(manager.getEpochOffsets(1L)) thenReturn Future.successful(Iterable(Partition(3,1339)))

    testKafkaSource.setRuntimeContext(runtimeContext)
    testKafkaSource.initializeState(initCtx)

    //Act
    testKafkaSource.cancel()
    testKafkaSource.snapshotState(context)
    val before = testKafkaSource.running
    testKafkaSource.poll(ctx)
    testKafkaSource.notifyCheckpointComplete(2)
    val after =testKafkaSource.running


    //Assert
    assert(before)
    assert(!after)
    assert(testKafkaSource.currentOffsets(3) == 1339)
  }

  it should "not close if the offsets had not been reached" in async {
    //Arrange
    val testKafkaSource = constructSource()
    val epochCollectionNodeMock = mock[EpochCollectionNode]
    val p = Promise[Unit]()
    when(consumer.poll(ctx)).thenAnswer(awaitAndReturn(p, Map(3 -> 1338L),2))
    when(consumer.getCurrentOffsets) thenReturn mutable.Map(3 -> 0L)
    //Initialize with empty collection
    testKafkaSource.setRuntimeContext(runtimeContext)
    testKafkaSource.initializeState(initCtx)


    val context = mock[FunctionSnapshotContext]
    when(context.getCheckpointId) thenReturn 1

    when(subjectNode.getEpochs()) thenReturn epochCollectionNodeMock
    when(epochCollectionNodeMock.getLatestEpochId()) thenReturn Future.successful(1L)
    when(manager.getEpochOffsets(1L)) thenReturn Future.successful(Iterable(Partition(3,1339)))

    //Act
    testKafkaSource.cancel()
    testKafkaSource.snapshotState(context)
    val before = testKafkaSource.running
    testKafkaSource.poll(ctx)
    testKafkaSource.notifyCheckpointComplete(1)
    val after =testKafkaSource.running


    //Assert
    assert(before)
    assert(after)
    assert(testKafkaSource.currentOffsets(3) == 1338)
  }

  it should "Swich to catchingUp state when receiving prepareSynchronize command" in async {
    //Arrange
    val testKafkaSource = constructSource()

    //Act
    testKafkaSource.apply(SourceCommand(KafkaSourceCommand.startSynchronize,None))

    //Assert
    verify(manager,times(1)).startedCatchingUp()
    assert(testKafkaSource.getState == KafkaSourceState.CatchingUp)
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
    when(manager.notifyStartedOnEpoch(ArgumentMatchers.any())) thenReturn Future.successful()

    //Act
    testKafkaSource.snapshotState(context)

    //Assert
    verify(manager, times(1)).notifyStartedOnEpoch(1337L)
    assert(true)
  }


  /**
    * Constructs a kafkaSource in the ready state
    */
  def constructSourceReady(): TestKafkaSource = {
    val source = constructSourceCatchingUp()
    val context = mock[FunctionSnapshotContext]
    when(context.getCheckpointId) thenReturn -100L
    when(manager.isCatchedUp(ArgumentMatchers.any())) thenReturn Future.successful(true)
    source.snapshotState(context)
    source
  }

  /**
    * Constructs a kafkaSource in the catching up state
    * @return
    */
  def constructSourceCatchingUp(): TestKafkaSource = {
    val source = constructSource()
    source.apply(SourceCommand(KafkaSourceCommand.startSynchronize,None))
    source
  }

  def constructSource(): TestKafkaSource = {
    val source = new TestKafkaSource(subjectNode,consumerFactory,consumer)
    //Override the default manager
    source.manager = manager
    source
  }

  /**
    * Runs the kafkaSource in a seperate thread and returns the thread that it is using
    * @param source source to run
    * @return
    */
  def runAsync(source: TestKafkaSource): Unit = {
    thread = new Thread {
      override def run() {
        source.setRuntimeContext(runtimeContext)
        source.initializeState(initCtx)
        source.run(ctx)
      }
    }
    thread.start()
  }




  override def afterEach(): Unit = {
    super.afterEach()
    //Clean up the worker thread if it was created
    if(thread != null) {
      thread.interrupt()
    }

  }




}



class SampleObject {

}

class TestKafkaSource(node: SubjectNode,kafkaConsumerFactory: KafkaConsumerFactory, mockedConsumer:KafkaSourceConsumer[SampleObject]) extends KafkaSource[SampleObject](node,kafkaConsumerFactory) {

  override val sourceUuid: String = "testuuid"

  override def mapToT(record: TrailedRecord): SampleObject = new SampleObject

  @transient override lazy val consumer: KafkaSourceConsumer[SampleObject] = mockedConsumer

  /**
    * Get typeinformation of the returned type
    *
    * @return
    */
  override def getProducedType: TypeInformation[SampleObject] = null
}




