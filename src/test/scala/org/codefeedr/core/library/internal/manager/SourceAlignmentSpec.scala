package org.codefeedr.core.library.internal.manager

import org.codefeedr.core.MockedLibraryServices
import org.codefeedr.core.library.internal.kafka.source.KafkaSourceState
import org.codefeedr.core.library.metastore._
import org.codefeedr.core.library.metastore.sourcecommand.{KafkaSourceCommand, SourceCommand}
import org.codefeedr.model.zookeeper.Partition
import org.codefeedr.util.MockitoExtensions
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterEach}

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}



class SourceAlignmentSpec  extends AsyncFlatSpec with MockitoSugar with BeforeAndAfterEach with MockedLibraryServices with MockitoExtensions {

  private var sourceNode: QuerySourceNode = _
  private var syncStateNode: SourceSynchronizationStateNode = _
  private var commandNode: QuerySourceCommandNode = _



  "startAlignment" should "lock on the sourceNode" in async {
    //Arrange
    val component = getComponent()

    //Act
    await(component.startAlignment())

    //Assert
    verify(sourceNode,times(1)).asyncWriteLock(ArgumentMatchers.any())
    assert(true)
  }

  it should "set the node state to catching up" in async {
    //Arrange
    val component = getComponent()

    //Act
    await(component.startAlignment())

    //Assert
    verify(syncStateNode, times(1)).setData(SynchronizationState(KafkaSourceState.CatchingUp))
    assert(true)
  }

  it should "push the synchronization command" in async {
    //Arrange
    val component = getComponent()

    //Act
    await(component.startAlignment())

    //Assert
    verify(commandNode, times(1)).push(SourceCommand(KafkaSourceCommand.startSynchronize))
    assert(true)
  }

  it should "throw an exception if the source state was not unsynchronized" in {
    //Arrange
    when(syncStateNode.getData()) thenReturn Future.successful(Some(SynchronizationState(KafkaSourceState.CatchingUp)))
    val component = getComponent()

    //Act
    val f = component.startAlignment()

    //Assert
    recoverToSucceededIf[Exception](f)
  }





  override def beforeEach(): Unit = {
    sourceNode = mock[QuerySourceNode]
    syncStateNode = mock[SourceSynchronizationStateNode]
    commandNode = mock[QuerySourceCommandNode]

    mockLock(sourceNode)

    when(sourceNode.getSyncState()) thenReturn syncStateNode
    when(sourceNode.getCommandNode()) thenReturn commandNode

    //Some default values
    when(syncStateNode.getData()) thenReturn Future.successful(Some(SynchronizationState(KafkaSourceState.UnSynchronized)))
    when(syncStateNode.setData(ArgumentMatchers.any())) thenReturn Future.successful()

    super.beforeEach()
  }

  private def getComponent(): SourceAlignment = new SourceAlignment(sourceNode)

}
