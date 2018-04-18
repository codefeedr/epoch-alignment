package org.codefeedr.core.library.internal.kafka.sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.types.Row
import org.codefeedr.core.MockedLibraryServices
import org.codefeedr.core.library.metastore._
import org.codefeedr.model.zookeeper.QuerySink
import org.codefeedr.model.{Record, RecordSourceTrail, SubjectType, TrailedRecord}
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterEach}

//Mockito
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.mockito.MockitoSugar

//Async
import scala.async.Async.{async, await}
import scala.collection.mutable
import scala.concurrent.{Future, Promise}


class KafkaSinkSpec  extends AsyncFlatSpec with MockitoSugar with BeforeAndAfterEach with MockedLibraryServices {
  var subjectType: SubjectType = _
  var subjectNode: SubjectNode = _
  var sinkCollectionNode: QuerySinkCollection = _
  var sinkNode: QuerySinkNode = _
  var producerCollectionNode: ProducerCollection = _
  var producerNode: ProducerNode = _

  var producerFactory: KafkaProducerFactory = _
  var configuration:Configuration = _



  "KafkaSink.open" should "create sink and producer node and put the producer in the true state" in async {
    //Arrange
    val sink = getTestSink()

    //Act
    sink.open(configuration)

    //Assert
    verify(sinkNode, times(1)).create(QuerySink("testsink"))
    verify(producerNode, times(1)).create(ArgumentMatchers.any())
    verify(producerNode, times(1)).setState(true)

    assert(true)
  }




  private def getTestSink(): TestKafkaSink = {
    new TestKafkaSink(subjectNode,producerFactory)
  }

  /**
    * Create new mock objects in beforeEach
    */
  override def beforeEach(): Unit = {
    subjectNode = mock[SubjectNode]
    subjectType = mock[SubjectType]
    sinkCollectionNode = mock[QuerySinkCollection]
    sinkNode = mock[QuerySinkNode]
    producerCollectionNode = mock[ProducerCollection]
    producerNode = mock[ProducerNode]

    configuration = mock[Configuration]
    producerFactory = mock[KafkaProducerFactory]

    when(subjectNode.exists()) thenReturn Future.successful(true)
    when(subjectNode.getDataSync()) thenReturn Some(subjectType)
    when(subjectNode.getSinks()) thenReturn sinkCollectionNode

    when(sinkCollectionNode.getChild("testsink")) thenReturn sinkNode
    when(sinkNode.getProducers()) thenReturn producerCollectionNode
    when(sinkNode.create(ArgumentMatchers.any())) thenReturn Future.successful(null)
    when(producerCollectionNode.getChild(ArgumentMatchers.any[String]())) thenReturn producerNode
    when(producerNode.create(ArgumentMatchers.any()))thenReturn Future.successful(null)
    when(producerNode.setState(ArgumentMatchers.any())) thenReturn Future.successful()
    when(subjectType.name) thenReturn "testsubject"
  }


}


class SampleObject {

}

class TestKafkaSink(node:SubjectNode, kafkaProducerFactory: KafkaProducerFactory) extends KafkaSink[SampleObject](node,kafkaProducerFactory)  {

  override protected val sinkUuid: String = "testsink"

  /**
    * Transform the sink type into the type that is actually sent to kafka
    *
    * @param value
    * @return
    */
  override def transform(value: SampleObject): (RecordSourceTrail, Row) = null
}

