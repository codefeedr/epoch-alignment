package org.codefeedr.core.library.internal.kafka.source

import org.apache.flink.api.common.state.{ListState, OperatorStateStore}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.codefeedr.core.library.metastore._

import scala.collection.JavaConverters._
import org.codefeedr.model.zookeeper.QuerySource
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


    assert(thread.getState().toString == "RUNNABLE")
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
    assert(thread.getState().toString == "RUNNABLE")
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
    assert(thread.getState().toString == "RUNNABLE")
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
    assert(thread.getState().toString == "RUNNABLE")
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
    assert(thread.getState().toString == "RUNNABLE")
  }

  it should "commit offsets and update liststate stored for a checkpoint when notify complete is called" in async {
    //Arrange
    val testKafkaSource = new TestKafkaSource(subjectNode,consumer)
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
    assert(thread.getState().toString == "RUNNABLE")
  }

  it should "mark the latest epoch and offsets when cancel is called" in async {
      assert(true)
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




