package org.codefeedr.core.library.internal.kafka.sink

import org.codefeedr.core.library.metastore._
import org.codefeedr.model.zookeeper.Partition
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.async.Async.{async, await}
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterEach}
import org.scalatest.mockito.MockitoSugar

import scala.collection.mutable
import scala.concurrent.Future

class EpochStateSpec extends AsyncFlatSpec with MockitoSugar with BeforeAndAfterEach {

  var epochCollectionNode : EpochCollectionNode = _
  var epochNode : EpochNode = _
  var epochPartitions : EpochPartitionCollection = _

  override def beforeEach(): Unit = {
    epochCollectionNode = mock[EpochCollectionNode]
    epochNode = mock[EpochNode]
    epochPartitions = mock[EpochPartitionCollection]

    when(epochCollectionNode.getChild("10")) thenReturn epochNode
    when(epochNode.exists()) thenReturn Future(true)
    when(epochNode.getPartitions()) thenReturn epochPartitions
  }

  "EpochState.PreCommit()" should "Create the partition nodes and dependencies in zookeeper" in async {
    //Arrange
    val transactionState = new TransactionState(0,10,0,false,mutable.Map(("",1) -> 10L, ("",2) -> 12L))
    val p1 = mock[EpochPartition]
    val p2 = mock[EpochPartition]
    when(epochPartitions.getChild("1")) thenReturn p1
    when(epochPartitions.getChild("2")) thenReturn p2

    when(p1.create(Partition(1,10l))) thenReturn Future.successful(Partition(1,10l))
    when(p2.create(Partition(2,12l))) thenReturn Future.successful(Partition(2,12l))

    val EpochState = new EpochState(transactionState,epochCollectionNode)


    //Act
    await(EpochState.preCommit())


    //Assert
    verify(p1).create(Partition(1,10l))
    verify(p2).create(Partition(2,12l))
    verify(epochNode, never()).create()
    verify(epochPartitions, never()).create()
    verify(p1,never()).setState(true)
    verify(p2, never()).setState(true)

    assert(true)
  }

  it should "Also create the epoch node if it does not exist" in async {
    //Arrange
    val transactionState = new TransactionState(0,10,0,false,mutable.Map(("",1) -> 10L, ("",2) -> 12L))
    val p1 = mock[EpochPartition]
    val p2 = mock[EpochPartition]
    when(epochNode.exists()) thenReturn Future(false)

    when(epochNode.create()) thenReturn Future.successful("")
    when(epochPartitions.create()) thenReturn Future.successful("")
    when(epochPartitions.getChild("1")) thenReturn p1
    when(epochPartitions.getChild("2")) thenReturn p2

    when(epochCollectionNode.asyncWriteLock(ArgumentMatchers.any[() => Future[Unit]]()))
      .thenAnswer(answer(a => a.getArgument[() => Future[Unit]](0)()))

    when(p1.create(Partition(1,10l))) thenReturn Future.successful(Partition(1,10l))
    when(p2.create(Partition(2,12l))) thenReturn Future.successful(Partition(2,12l))

    val EpochState = new EpochState(transactionState,epochCollectionNode)


    //Act
    await(EpochState.preCommit())

    //Assert
    verify(p1).create(Partition(1,10l))
    verify(p2).create(Partition(2,12l))
    verify(epochNode).create()
    verify(epochPartitions).create()
    verify(p1,never()).setState(true)
    verify(p2, never()).setState(true)

    assert(true)
  }


  "EpochState.Commit()" should "Set all EpochPartitions to true" in async {
    //Arrange
    val transactionState = new TransactionState(0,10,0,false,mutable.Map(("",1) -> 10L, ("",2) -> 12L))
    val p1 = mock[EpochPartition]
    val p2 = mock[EpochPartition]
    when(epochPartitions.getChild("1")) thenReturn p1
    when(epochPartitions.getChild("2")) thenReturn p2


    when(p1.create(Partition(1,10l))) thenReturn Future.successful(Partition(1,10l))
    when(p2.create(Partition(2,12l))) thenReturn Future.successful(Partition(2,12l))
    when(p1.setState(true)) thenReturn Future.successful()
    when(p2.setState(true)) thenReturn Future.successful()

    val EpochState = new EpochState(transactionState,epochCollectionNode)

    //Act
    await(EpochState.preCommit())
    await(EpochState.commit())

    //Assert
    verify(p1, times(1)).setState(true)
    verify(p2, times(1)).setState(true)

    assert(true)
  }



  def answer[T](f: InvocationOnMock => T): Answer[T] = {
    //Ignore the warning, compiler needs it
    new Answer[T] {
      override def answer(invocation: InvocationOnMock): T = f(invocation)
    }
  }

}
