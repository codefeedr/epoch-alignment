package org.codefeedr.core.library.internal.kafka.source

import org.apache.flink.api.common.state.{ListState, OperatorStateStore}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.codefeedr.core.library.metastore._

import scala.collection.JavaConverters._
import org.codefeedr.model.zookeeper.{Partition, QuerySource}
import org.codefeedr.model.{RecordProperty, SubjectType, TrailedRecord}
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterEach}
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.mockito.MockitoSugar

import scala.async.Async.{async, await}
import scala.collection.mutable
import scala.concurrent.{Future, Promise}

class KafkaSourceSpec extends AsyncFlatSpec with MockitoSugar with BeforeAndAfterEach {

  private var subjectNode: SubjectNode = _
  private var ctx: SourceFunction.SourceContext[SampleObject] = _
  private var closePromise: Promise[Unit] = _
  private var sampleObject: SampleObject = _

  private var consumer: KafkaSourceConsumer[SampleObject] = _

  private var sourceCollectionNode : QuerySourceCollection= _
  private var sourceNode: QuerySourceNode = _

  private var consumerCollection: ConsumerCollection = _
  private var consumerNode: ConsumerNode = _

  private var initCtx : FunctionInitializationContext  = _
  private var operatorStore: OperatorStateStore = _
  private var listState:ListState[(Int, Long)] = _

  private var runtimeContext:StreamingRuntimeContext = _

  private var cpLock:Object = _

  private var thread:Thread = _


  override def beforeEach(): Unit = {
    subjectNode = mock[SubjectNode]
    ctx = mock[SourceFunction.SourceContext[SampleObject]]
    sampleObject = new SampleObject()

    consumer = mock[KafkaSourceConsumer[SampleObject]]

    sourceCollectionNode = mock[QuerySourceCollection]
    sourceNode = mock[QuerySourceNode]

    consumerCollection = mock[ConsumerCollection]
    consumerNode = mock[ConsumerNode]

    initCtx = mock[FunctionInitializationContext]
    operatorStore = mock[OperatorStateStore]
    listState = mock[ListState[(Int, Long)]]

    runtimeContext = mock[StreamingRuntimeContext]

    cpLock = new Object()

    when(subjectNode.getDataSync()) thenReturn
      Some(SubjectType("subjectuuid", "SampleObject",false, new Array[RecordProperty[_]](0)))

    when(subjectNode.getSources()) thenReturn sourceCollectionNode
    when(sourceCollectionNode.getChild("testuuid")) thenReturn sourceNode
    when(sourceNode.create(ArgumentMatchers.any())) thenReturn Future.successful(null)

    when(sourceNode.getConsumers()) thenReturn consumerCollection
    when(consumerCollection.getChild(ArgumentMatchers.any[String]())) thenReturn consumerNode
    when(consumerNode.create(ArgumentMatchers.any())) thenReturn Future.successful(null)

    closePromise = Promise[Unit]()
    when(subjectNode.awaitClose()) thenReturn closePromise.future


    when(initCtx.getOperatorStateStore) thenReturn operatorStore
    when(operatorStore.getListState[(Int, Long)](ArgumentMatchers.any())) thenReturn listState
    when(listState.get()) thenReturn List[(Int, Long)]().asJava

    when(consumer.getCurrentOffsets()) thenReturn mutable.Map[Int,Long]()
    when(consumer.poll(ArgumentMatchers.any())) thenReturn Map[Int, Long]()

    when(runtimeContext.isCheckpointingEnabled) thenReturn true

    when(ctx.getCheckpointLock()).thenReturn (cpLock,cpLock)
  }


  "KafkaSource.Run" should "Create consumer and source nodes" in async {
    //Arrange
    val testKafkaSource = new TestKafkaSource(subjectNode,consumer)


    //Act
    runAsync(testKafkaSource)
    while(!testKafkaSource.intitialized){}

    //Assert
    verify(sourceNode, times(1)).create(ArgumentMatchers.any())
    verify(consumerNode, times(1)).create(ArgumentMatchers.any())


    assert(testKafkaSource.running)
  }

  it should "initialize with startoffsets from the consumer" in async {
    //Arrange
    val testKafkaSource = new TestKafkaSource(subjectNode,consumer)
    when(consumer.getCurrentOffsets()) thenReturn mutable.Map[Int, Long](1 -> 10,2->13)

    //Act
    runAsync(testKafkaSource)
    while(!testKafkaSource.intitialized){}

    //Assert
    verify(sourceNode, times(1)).create(ArgumentMatchers.any())
    verify(consumerNode, times(1)).create(ArgumentMatchers.any())

    assert(testKafkaSource.currentOffsets(2) == 13L)
    assert(testKafkaSource.currentOffsets(1) == 10L)
    assert(testKafkaSource.running)
  }


  it should "store the current offsets when snapshotState is called" in async {
    //Arrange
    val testKafkaSource = new TestKafkaSource(subjectNode,consumer)
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
    val testKafkaSource = new TestKafkaSource(subjectNode,consumer)

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
    val testKafkaSource = new TestKafkaSource(subjectNode,consumer)
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
    val testKafkaSource = new TestKafkaSource(subjectNode,consumer)
    testKafkaSource.setRuntimeContext(runtimeContext)
    testKafkaSource.initializeState(initCtx)
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
    val testKafkaSource = new TestKafkaSource(subjectNode,consumer)
    testKafkaSource.setRuntimeContext(runtimeContext)
    testKafkaSource.initializeState(initCtx)
    val epochCollectionNodeMock = mock[EpochCollectionNode]
    val finalEpochMock = mock[EpochNode]

    when(subjectNode.getEpochs()) thenReturn epochCollectionNodeMock
    when(epochCollectionNodeMock.getLatestEpochId()) thenReturn Future.successful(1337)
    when(epochCollectionNodeMock.getChild("1337")) thenReturn finalEpochMock
    when(finalEpochMock.getPartitionData()) thenReturn Future.successful(List(Partition(2,1338)))

    //Act
    testKafkaSource.cancel()

    //Assert
    assert(testKafkaSource.finalSourceEpoch == 1337)
    assert(testKafkaSource.finalSourceEpochOffsets(2) == 1338)
  }

  it should "Use the current offsets of a subject when cancel is called on a subject that has no epochs" in async {
    //Arrange
    val testKafkaSource = new TestKafkaSource(subjectNode,consumer)
    testKafkaSource.setRuntimeContext(runtimeContext)
    testKafkaSource.initializeState(initCtx)
    val epochCollectionNodeMock = mock[EpochCollectionNode]
    val finalEpochMock = mock[EpochNode]

    when(subjectNode.getEpochs()) thenReturn epochCollectionNodeMock
    when(epochCollectionNodeMock.getLatestEpochId()) thenReturn Future.successful(-1)
    when(consumer.getEndOffsets()) thenReturn Map(3 -> 1339L)

    //Act
    testKafkaSource.cancel()

    //Assert
    assert(testKafkaSource.finalSourceEpoch == -1)
    assert(testKafkaSource.finalSourceEpochOffsets(3) == 1339)
  }


  it should "Close the source when a poll obtained all data of the final offsets" in async {
    //Arrange
    val testKafkaSource = new TestKafkaSource(subjectNode,consumer)
    testKafkaSource.setRuntimeContext(runtimeContext)
    testKafkaSource.initializeState(initCtx)
    val epochCollectionNodeMock = mock[EpochCollectionNode]
    val finalEpochMock = mock[EpochNode]
    val p = Promise[Unit]()
    when(consumer.poll(ctx)).thenAnswer(awaitAndReturn(p, Map(3 -> 1339L),2))

    val context = mock[FunctionSnapshotContext]
    when(context.getCheckpointId) thenReturn 1


    when(subjectNode.getEpochs()) thenReturn epochCollectionNodeMock
    when(epochCollectionNodeMock.getLatestEpochId()) thenReturn Future.successful(-1)
    when(consumer.getEndOffsets()) thenReturn Map(3 -> 1339L)

    //Act
    testKafkaSource.cancel()
    testKafkaSource.snapshotState(context)
    val before = testKafkaSource.running
    testKafkaSource.poll(ctx)
    testKafkaSource.notifyCheckpointComplete(1)
    val after =testKafkaSource.running


    //Assert
    assert(before)
    assert(!after)
    assert(testKafkaSource.currentOffsets(3) == 1339)
  }

  it should "not close if the offsets had not been reached" in async {
    //Arrange
    val testKafkaSource = new TestKafkaSource(subjectNode,consumer)
    testKafkaSource.setRuntimeContext(runtimeContext)
    val epochCollectionNodeMock = mock[EpochCollectionNode]
    val finalEpochMock = mock[EpochNode]
    val p = Promise[Unit]()
    when(consumer.poll(ctx)).thenAnswer(awaitAndReturn(p, Map(3 -> 1338L),2))
    when(consumer.getCurrentOffsets()) thenReturn mutable.Map(3 -> 0L)
    //Initialize with empty collection
    testKafkaSource.initializeState(initCtx)

    val context = mock[FunctionSnapshotContext]
    when(context.getCheckpointId) thenReturn 1

    when(subjectNode.getEpochs()) thenReturn epochCollectionNodeMock
    when(epochCollectionNodeMock.getLatestEpochId()) thenReturn Future.successful(-1)
    when(consumer.getEndOffsets()) thenReturn Map(3 -> 1339L)

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

  /**
    * Runs the kafkaSource in a seperate thread and returns the thread that it is using
    * @param source
    * @return
    */
  def runAsync(source: TestKafkaSource): Unit = {
    thread = new Thread {
      override def run {
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


  def answer[T](f: InvocationOnMock => T): Answer[T] = {
    //Ignore the warning, compiler needs it
    new Answer[T] {
      override def answer(invocation: InvocationOnMock): T = f(invocation)
    }
  }

  def awaitAndReturn[T](p:Promise[Unit], a: T,times:Int) = awaitAndAnswer(p,_=>a,times)

  def awaitAndAnswer[T](p:Promise[Unit], f: InvocationOnMock => T,times:Int): Answer[T] = {
    var i = times

    new Answer[T] {
      override def answer(invocation: InvocationOnMock): T = {
        i = i-1
        if(i == 0) {
          p.success()
        }
        f(invocation)
      }
    }
  }
}



class SampleObject {

}

class TestKafkaSource(node: SubjectNode, mockedConsumer: KafkaSourceConsumer[SampleObject]) extends KafkaSource[SampleObject](node) {
  override val sourceUuid: String = "testuuid"

  @transient override lazy val consumer = mockedConsumer

  override def mapToT(record: TrailedRecord): SampleObject = new SampleObject

  /**
    * Get typeinformation of the returned type
    *
    * @return
    */
  override def getProducedType: TypeInformation[SampleObject] = ???
}




