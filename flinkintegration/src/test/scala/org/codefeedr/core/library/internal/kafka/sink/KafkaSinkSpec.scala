package org.codefeedr.core.library.internal.kafka.sink

import java.lang

import org.apache.flink.api.common.state.{ListState, OperatorStateStore}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.apache.flink.types.Row
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.codefeedr.core.MockedLibraryServices
import org.codefeedr.core.library.metastore._
import org.codefeedr.model.zookeeper.QuerySink
import org.codefeedr.model.{RecordSourceTrail, SubjectType}
import org.codefeedr.util.MockitoExtensions
import org.codefeedr.util.NoEventTime._
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterEach}

import scala.collection.JavaConverters._

//Mockito
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar

//Async
import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


class KafkaSinkSpec  extends AsyncFlatSpec with MockitoSugar with BeforeAndAfterEach with MockedLibraryServices with MockitoExtensions {
  var subjectType: SubjectType = _
  var subjectNode: SubjectNode = _
  var jobNode: JobNode = _

  var sinkCollectionNode: QuerySinkCollection = _
  var sinkNode: QuerySinkNode = _
  var producerCollectionNode: ProducerCollection = _
  var producerNode: ProducerNode = _

  var producerFactory: KafkaProducerFactory = _
  var configuration:Configuration = _

  var p0,p1,p2,p3,p4: KafkaProducer[RecordSourceTrail, Row] = _

  private var runtimeContext:StreamingRuntimeContext = _
  var initCtx:FunctionInitializationContext = _
  private var operatorStore: OperatorStateStore = _
  private var listState:ListState[(Int, Long)] = _

  private var epochStateManager:EpochStateManager = _

  private var context : SinkFunction.Context[_] = _


  "KafkaSink.open" should "create sink and producer node and put the producer in the true state" in async {
    //Arrange
    val sink = getTestSink()

    //Act
    sink.open(configuration)

    //Assert
    verify(sinkNode, times(1)).create(QuerySink("testsink"))
    verify(producerNode, times(1)).create(ArgumentMatchers.any())
    verify(producerNode, times(1)).setState(true)

    assert(sink.checkpointingMode.contains(CheckpointingMode.EXACTLY_ONCE))
  }


  "KafkaSink.BeginTransaction" should "Create an empty transaction with the first free producer of the pool" in async {
    //Arrange
    val sink = getTestSink()

    //Act
    val transaction = sink.beginTransaction()

    //Assert
    assert(transaction.offsetMap.isEmpty)
  }

  it should "Create a new empty transaction if BeginTransaction is called again" in async {
    //Arrange
    val sink = getTestSink()

    //Act

    val transaction1 = sink.beginTransaction()
    val transaction2 = sink.beginTransaction()

    //Assert
    assert(transaction2.offsetMap.isEmpty)
    assert(transaction1 != transaction2)
  }

  "KafkaSink.snapshotState" should "Set the checkpointId on the currentTransaction" in async {
    //Arrange
    val sink = getTestSink()
    val context = mock[FunctionSnapshotContext]
    when(context.getCheckpointId) thenReturn 10

    //Act
    val transaction1 = sink.currentTransaction()
    sink.snapshotState(context)
    val transaction2 = sink.currentTransaction()
    when(context.getCheckpointId) thenReturn 1337
    sink.snapshotState(context)

    //Assert
    assert(transaction1.checkPointId == 10)
    assert(transaction2.checkPointId == 1337)
  }


  "KafkaSink.Invoke" should "Emit the event to the currently assigned producer" in async {
    //Arrange
    val sink = getTestSink()
    val o = new SampleObject()
    val transaction = sink.currentTransaction()
    val producer = sink.producerPool(transaction.producerIndex)

    //Act
    sink.invoke(transaction,o,context)

    verify(producer, times(1)).send(ArgumentMatchers.any(),ArgumentMatchers.any())
    assert(true)
  }

  it should "Switch to the next producer on the next epoch" in async {
    //Arrange
    val snapshotContext = mock[FunctionSnapshotContext]
    val sink = getTestSink()
    val o = new SampleObject()
    val transaction1 = sink.currentTransaction()
    val transaction2 = sink.beginTransaction()
    val transaction3 = sink.beginTransaction()
    val producer1 = sink.producerPool(transaction1.producerIndex)
    val producer2 = sink.producerPool(transaction2.producerIndex)
    val producer3 = sink.producerPool(transaction3.producerIndex)

    //Act
    sink.invoke(transaction1,o,context)
    sink.invoke(transaction2,o,context)

    //Assert
    assert(producer1 != producer2)
    assert(producer1 != producer3)
    assert(producer2 != producer3)
    verify(producer1, times(1)).send(ArgumentMatchers.any(),ArgumentMatchers.any())
    verify(producer2, times(1)).send(ArgumentMatchers.any(),ArgumentMatchers.any())
    verify(producer3, times(0)).send(ArgumentMatchers.any(),ArgumentMatchers.any())
    assert(true)
  }

  it should "Update the transaction state with new offsets" in async {
    //Arrange
    val snapshotContext = mock[FunctionSnapshotContext]
    val sink = getTestSink()
    val o = new SampleObject()
    val transaction1 = sink.currentTransaction()
    val producer1 = sink.producerPool(transaction1.producerIndex)
    var cb: Callback = null

    val mockedMetadata = new RecordMetadata(new TopicPartition("",2),0L,1337L,0L,new lang.Long(0),0,0)
    when(producer1.send(ArgumentMatchers.any(),ArgumentMatchers.any())) thenAnswer answer[java.util.concurrent.Future[RecordMetadata]](r => {
      cb = r.getArgument[Callback](1)
      null
    })

    //Act
    sink.invoke(transaction1,o,context)
    val pendingBefore = transaction1.pendingEvents
    cb.onCompletion(mockedMetadata, null)
    //Need to await, because this gets completed asynchronous
    await(transaction1.awaitCommit())
    val pendingAfter = transaction1.pendingEvents

    //Assert
    assert(cb != null)
    assert(pendingBefore == 1)
    assert(pendingAfter == 0)
    assert(transaction1.offsetMap(2) == 1337)
  }

  "KafkaSink.PreCommit()" should "call preCommit on the epochStateManager" in async {
    //Arrange
    val sink = getTestSink()
    val o = new SampleObject()
    val transaction1 = sink.currentTransaction()
    val producer1 = sink.producerPool(transaction1.producerIndex)
    var cb: Callback = null

    val mockedMetadata = new RecordMetadata(new TopicPartition("",2),0L,1337L,0L,new lang.Long(0),0,0)
    when(producer1.send(ArgumentMatchers.any(),ArgumentMatchers.any())) thenAnswer answer[java.util.concurrent.Future[RecordMetadata]](r => {
      cb = r.getArgument[Callback](1)
      null
    })


    //Act
    sink.invoke(transaction1,o,context)
    cb.onCompletion(mockedMetadata,null)
    sink.preCommit(transaction1)


    //Assert
    verify(epochStateManager, times(1)).preCommit(ArgumentMatchers.any())
    verify(epochStateManager, times(0)).commit(ArgumentMatchers.any())
    assert(true)

  }

  it should "Flush the transaction to kafka" in async {
    //Arrange
    val sink = getTestSink()
    val o = new SampleObject()
    val transaction1 = sink.currentTransaction()
    val producer1 = sink.producerPool(transaction1.producerIndex)
    var cb: Callback = null

    val mockedMetadata = new RecordMetadata(new TopicPartition("",2),0L,1337L,0L,new lang.Long(0),0,0)
    when(producer1.send(ArgumentMatchers.any(),ArgumentMatchers.any())) thenAnswer answer[java.util.concurrent.Future[RecordMetadata]](r => {
      cb = r.getArgument[Callback](1)
      null
    })



    //Act
    sink.invoke(transaction1,o,context)
    cb.onCompletion(mockedMetadata,null)
    sink.preCommit(transaction1)

    //Assert
    //verify(producer1, times(1)).flush()
    verify(producer1, times(0)).commitTransaction()
    assert(transaction1.awaitCommit().isCompleted)
  }

  "KafkaSink.Commit()" should "Flag the created transaction in zookeeper as committed" in async {
    //Arrange
    val sink = getTestSink()
    val o = new SampleObject()
    val transaction1 = sink.currentTransaction()
    val producer1 = sink.producerPool(transaction1.producerIndex)
    var cb: Callback = null

    val mockedMetadata = new RecordMetadata(new TopicPartition("",2),0L,1337L,0L,new lang.Long(0),0,0)
    when(producer1.send(ArgumentMatchers.any(),ArgumentMatchers.any())) thenAnswer answer[java.util.concurrent.Future[RecordMetadata]](r => {
      cb = r.getArgument[Callback](1)
      null
    })


    //Act
    sink.invoke(transaction1,o,context)
    cb.onCompletion(mockedMetadata,null)
    sink.preCommit(transaction1)
    sink.commit(transaction1)

    //Assert
    //verify(producer1, times(1)).flush()
    verify(producer1, times(1)).commitTransaction()
    assert(true)
  }

  it should "Commit the transaction to kafka" in async {
    //Arrange
    val sink = getTestSink()
    val o = new SampleObject()
    val transaction1 = sink.currentTransaction()
    val producer1 = sink.producerPool(transaction1.producerIndex)
    var cb: Callback = null

    val mockedMetadata = new RecordMetadata(new TopicPartition("",2),0L,1337L,0L, new lang.Long(0),0,0)
    when(producer1.send(ArgumentMatchers.any(),ArgumentMatchers.any())) thenAnswer answer[java.util.concurrent.Future[RecordMetadata]](r => {
      cb = r.getArgument[Callback](1)
      null
    })


    //Act
    sink.invoke(transaction1,o,context)
    cb.onCompletion(mockedMetadata,null)
    sink.preCommit(transaction1)
    sink.commit(transaction1)

    //Assert
    verify(epochStateManager, times(1)).preCommit(ArgumentMatchers.any())
    verify(epochStateManager, times(1)).commit(ArgumentMatchers.any())
    assert(true)
  }

  /**
    * Constructs a sink and places it and its base class into a representation of an initial state
    * @return
    */
  private def getTestSink(): TestKafkaSink = {
    val sink = new TestKafkaSink(subjectNode,jobNode,producerFactory,epochStateManager)
    sink.setRuntimeContext(runtimeContext)
    sink.initializeState(initCtx)
    sink
  }

  /**
    * Create new mock objects in beforeEach
    */
  override def beforeEach(): Unit = {
    super.beforeEach()
    subjectNode = mock[SubjectNode]
    jobNode = mock[JobNode]
    subjectType = mock[SubjectType]
    sinkCollectionNode = mock[QuerySinkCollection]
    sinkNode = mock[QuerySinkNode]
    producerCollectionNode = mock[ProducerCollection]
    producerNode = mock[ProducerNode]

    configuration = mock[Configuration]
    producerFactory = mock[KafkaProducerFactory]

    runtimeContext = mock[StreamingRuntimeContext]
    initCtx = mock[FunctionInitializationContext]
    operatorStore = mock[OperatorStateStore]
    listState = mock[ListState[(Int, Long)]]

    context = mock[SinkFunction.Context[_]]

    epochStateManager = mock[EpochStateManager]

    p0 = mock[KafkaProducer[RecordSourceTrail, Row]]
    p1 = mock[KafkaProducer[RecordSourceTrail, Row]]
    p2 = mock[KafkaProducer[RecordSourceTrail, Row]]
    p3 = mock[KafkaProducer[RecordSourceTrail, Row]]
    p4 = mock[KafkaProducer[RecordSourceTrail, Row]]


    when(subjectNode.exists()) thenReturn Future.successful(true)
    when(subjectNode.getDataSync()) thenReturn Some(subjectType)
    when(subjectNode.getSinks()) thenReturn sinkCollectionNode

    when(sinkCollectionNode.getChild("testsink")) thenReturn sinkNode
    when(sinkNode.getProducers()) thenReturn producerCollectionNode
    when(sinkNode.create(ArgumentMatchers.any())) thenReturn Future.successful(null)
    when(producerCollectionNode.getChild(ArgumentMatchers.any[String]())) thenReturn producerNode
    when(producerNode.create(ArgumentMatchers.any()))thenReturn Future.successful(null)
    when(producerNode.setState(ArgumentMatchers.any())) thenReturn Future.successful(())
    when(subjectType.name) thenReturn "testsubject"

    when(runtimeContext.isCheckpointingEnabled) thenReturn true
    when(initCtx.getOperatorStateStore) thenReturn operatorStore
    when(initCtx.isRestored) thenReturn false
    when(operatorStore.getListState[(Int, Long)](ArgumentMatchers.any())) thenReturn listState
    when(listState.get()) thenReturn List[(Int, Long)]().asJava

    when(producerFactory.create[RecordSourceTrail, Row](ArgumentMatchers.endsWith("1"))(ArgumentMatchers.any(),ArgumentMatchers.any())) thenReturn p0
    when(producerFactory.create[RecordSourceTrail, Row](ArgumentMatchers.endsWith("2"))(ArgumentMatchers.any(),ArgumentMatchers.any())) thenReturn p1
    when(producerFactory.create[RecordSourceTrail, Row](ArgumentMatchers.endsWith("3"))(ArgumentMatchers.any(),ArgumentMatchers.any())) thenReturn p2
    when(producerFactory.create[RecordSourceTrail, Row](ArgumentMatchers.endsWith("4"))(ArgumentMatchers.any(),ArgumentMatchers.any())) thenReturn p3
    when(producerFactory.create[RecordSourceTrail, Row](ArgumentMatchers.endsWith("5"))(ArgumentMatchers.any(),ArgumentMatchers.any())) thenReturn p4

    when(epochStateManager.commit(ArgumentMatchers.any())) thenReturn Future.successful(())
    when(epochStateManager.preCommit(ArgumentMatchers.any())) thenReturn Future.successful(())
  }

}


class SampleObject {

}

class TestKafkaSink(node:SubjectNode, jobNode: JobNode,kafkaProducerFactory: KafkaProducerFactory,epochStateManager:EpochStateManager) extends KafkaSink[SampleObject,Row,RecordSourceTrail](node,jobNode,kafkaProducerFactory,epochStateManager)  {

  override protected val sinkUuid: String = "testsink"

  /**
    * Transform the sink type into the type that is actually sent to kafka
    *
    * @param value
    * @return
    */
  override def transform(value: SampleObject): (RecordSourceTrail, Row) = (null,null)
}

