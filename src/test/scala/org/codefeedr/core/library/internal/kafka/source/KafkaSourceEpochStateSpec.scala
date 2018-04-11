package org.codefeedr.core.library.internal.kafka.source

import org.codefeedr.core.library.internal.kafka.meta.SourceEpoch
import org.codefeedr.core.library.metastore._
import org.codefeedr.model.zookeeper.Partition
import org.mockito.Mockito.{verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.{ArgumentMatchers, Mock}
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterEach}
import org.scalatest.mockito.MockitoSugar

import scala.async.Async.{async, await}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class KafkaSourceEpochStateSpec extends AsyncFlatSpec with MockitoSugar with BeforeAndAfterEach {


  private var sourceEpochCollection: SourceEpochCollection = _
  private var epochCollection: EpochCollectionNode = _
  private var subjectNode: SubjectNode = _
  private var sourceNode: QuerySourceNode = _

  private var kafkaSourceEpochState: KafkaSourceEpochState = _

  "nextSourceEpoch" should "return an existing value if already created in zookeeper" in async {
    //Arrange
    val sourceEpochNode = mock[SourceEpochNode]
    val sourceEpoch = SourceEpoch(List[Partition](),1337,1337)

    when(sourceEpochCollection.getChild("1")) thenReturn sourceEpochNode
    when(sourceEpochNode.exists()) thenReturn Future.successful(true)
    when(sourceEpochNode.getData()) thenReturn Future.successful(Some(sourceEpoch))


    //Act
    val data = await(kafkaSourceEpochState.nextSourceEpoch(1))

    //Assert
    assert(data == sourceEpoch)
  }

  it should "select offsets from the latest epoch of the source if no previous synchronisation happened" in async {
    //Arrange
    val sourceEpochNode = mock[SourceEpochNode]
    val previousSourceEpochNode = mock[SourceEpochNode]

    when(sourceEpochCollection.getChild("1")) thenReturn sourceEpochNode
    when(sourceEpochCollection.getChild("0")) thenReturn previousSourceEpochNode

    when(sourceEpochNode.exists()) thenReturn Future.successful(false)
    when(previousSourceEpochNode.exists()) thenReturn Future.successful(false)

    val children = List(mock[EpochNode], mock[EpochNode])
    val partitions = List(Partition(1,1),Partition(20,10))
    when(children(0).getEpoch()) thenReturn 0
    when(children(1).getEpoch()) thenReturn 1
    when(epochCollection.getChild("1")) thenReturn children(1)
    when(epochCollection.getLatestEpochId()) thenReturn (Future successful 1)

    when(children(1).getPartitionData()) thenReturn Future.successful(partitions)
    when(epochCollection.getChildren()) thenReturn Future.successful(children)

    when(sourceEpochNode.create(ArgumentMatchers.any[SourceEpoch]())) thenAnswer answer(a =>Future.successful(a.getArgument[SourceEpoch](0)))

    //Act
    val data = await(kafkaSourceEpochState.nextSourceEpoch(1))

    //Assert
    //verify node is created
    verify(sourceEpochNode).create(data)

    //Verify the data of the node
    assert(data.subjectEpochId == 1)
    assert(data.epochId == 1)
    assert(data.partitions(0).nr == 1)
    assert(data.partitions(0).offset == 1)
    assert(data.partitions(1).nr == 20)
    assert(data.partitions(1).offset == 10)
  }

  it should "Select the next epoch if there was a previous synchronised epoch" in async {
    //Arrange
    val sourceEpochNode = mock[SourceEpochNode]
    val previousSourceEpochNode = mock[SourceEpochNode]
    val ps = List(Partition(1,1),Partition(20,10))
    val previousSourceEpoch = SourceEpoch(ps, 0,10)
    val nextEpoch = mock[EpochNode]

    when(sourceEpochCollection.getChild("1")) thenReturn sourceEpochNode
    when(sourceEpochCollection.getChild("0")) thenReturn previousSourceEpochNode

    when(sourceEpochNode.exists()) thenReturn Future.successful(false)
    when(previousSourceEpochNode.exists()) thenReturn Future.successful(true)
    when(previousSourceEpochNode.getData()) thenReturn Future.successful(Some(previousSourceEpoch))

    when(epochCollection.getChild("11")) thenReturn nextEpoch
    when(nextEpoch.exists()) thenReturn Future.successful(true)
    when(nextEpoch.getPartitionData()) thenReturn Future.successful(ps)
    when(nextEpoch.getEpoch()) thenReturn 11

    when(sourceEpochNode.create(ArgumentMatchers.any[SourceEpoch]())) thenAnswer answer(a =>Future.successful(a.getArgument[SourceEpoch](0)))

    //Act
    val data = await(kafkaSourceEpochState.nextSourceEpoch(1))
    //verify node is created
    verify(sourceEpochNode).create(data)

    //Verify the data of the node
    assert(data.subjectEpochId == 11)
    assert(data.epochId == 1)
    assert(data.partitions(0).nr == 1)
    assert(data.partitions(0).offset == 1)
    assert(data.partitions(1).nr == 20)
    assert(data.partitions(1).offset == 10)
  }

  it should "Reuse the previous epoch from the subject if no new epoch is available" in async {
    //Arrange
    val sourceEpochNode = mock[SourceEpochNode]
    val previousSourceEpochNode = mock[SourceEpochNode]
    val ps = List(Partition(1,1),Partition(20,10))
    val previousSourceEpoch = SourceEpoch(ps, 0,10)
    val nextEpoch = mock[EpochNode]
    val previousEpoch = mock[EpochNode]

    when(sourceEpochCollection.getChild("1")) thenReturn sourceEpochNode
    when(sourceEpochCollection.getChild("0")) thenReturn previousSourceEpochNode

    when(sourceEpochNode.exists()) thenReturn Future.successful(false)
    when(previousSourceEpochNode.exists()) thenReturn Future.successful(true)
    when(previousSourceEpochNode.getData()) thenReturn Future.successful(Some(previousSourceEpoch))

    when(epochCollection.getChild("11")) thenReturn nextEpoch
    when(nextEpoch.exists()) thenReturn Future.successful(false)

    when(epochCollection.getChild("10")) thenReturn previousEpoch
    when(previousEpoch.getPartitionData()) thenReturn Future.successful(ps)
    when(previousEpoch.getEpoch()) thenReturn 10

    when(sourceEpochNode.create(ArgumentMatchers.any[SourceEpoch]())) thenAnswer answer(a =>Future.successful(a.getArgument[SourceEpoch](0)))

    //Act
    val data = await(kafkaSourceEpochState.nextSourceEpoch(1))
    //verify node is created
    verify(sourceEpochNode).create(data)

    //Verify the data of the node
    assert(data.subjectEpochId == 10)
    assert(data.epochId == 1)
    assert(data.partitions(0).nr == 1)
    assert(data.partitions(0).offset == 1)
    assert(data.partitions(1).nr == 20)
    assert(data.partitions(1).offset == 10)

  }


  override def beforeEach(): Unit = {
    sourceEpochCollection = mock[SourceEpochCollection]
    epochCollection = mock[EpochCollectionNode]
    subjectNode = mock[SubjectNode]
    sourceNode = mock[QuerySourceNode]


    when(sourceEpochCollection.asyncWriteLock(ArgumentMatchers.any[() => Future[Unit]]()))
      .thenAnswer(answer(a => a.getArgument[() => Future[Unit]](0)()))

    when(subjectNode.getEpochs()) thenReturn epochCollection
    when(sourceNode.getEpochs()) thenReturn sourceEpochCollection

    kafkaSourceEpochState = new KafkaSourceEpochState(subjectNode = subjectNode, querySourceNode = sourceNode)
  }


  def answer[T](f: InvocationOnMock => T): Answer[T] = {
    //Ignore the warning, compiler needs it
    new Answer[T] {
      override def answer(invocation: InvocationOnMock): T = f(invocation)
    }
  }

}
