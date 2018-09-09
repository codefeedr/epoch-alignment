package org.codefeedr.core.library.internal.manager

import org.codefeedr.core.MockedLibraryServices
import org.codefeedr.core.library.internal.kafka.source.KafkaSourceState
import org.codefeedr.core.library.metastore._
import org.codefeedr.core.library.metastore.sourcecommand.{KafkaSourceCommand, SourceCommand}
import org.codefeedr.util.{MockitoExtensions}
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterEach}

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future



class SourceAlignmentSpec  extends AsyncFlatSpec with MockitoSugar with BeforeAndAfterEach with MockedLibraryServices with MockitoExtensions {

  private var sourceNode: QuerySourceNode = _
  private var sourceEpochs: SourceEpochCollection = _
  private var syncStateNode: SourceSynchronizationStateNode = _
  private var commandNode: QuerySourceCommandNode = _



  "startAlignment" should "lock on the sourceNode" in async {
    //Arrange
    val component = constructComponent()

    //Act
    await(component.startAlignment())

    //Assert
    verify(sourceNode,times(1)).asyncWriteLock(ArgumentMatchers.any())
    assert(true)
  }

  it should "set the node state to catching up" in async {
    //Arrange
    val component = constructComponent()

    //Act
    await(component.startAlignment())

    //Assert
    verify(syncStateNode, times(1)).setData(SynchronizationState(KafkaSourceState.CatchingUp))
    assert(true)
  }

  it should "push the synchronization command" in async {
    //Arrange
    val component = constructComponent()

    //Act
    await(component.startAlignment())

    //Assert
    verify(commandNode, times(1)).push(SourceCommand(KafkaSourceCommand.catchUp,None))
    assert(true)
  }

  it should "throw an exception if the source state was not unsynchronized" in {
    //Arrange
    when(syncStateNode.getData()) thenReturn Future.successful(Some(SynchronizationState(KafkaSourceState.CatchingUp)))
    val component = constructComponent()

    //Act
    val f = component.startAlignment()

    //Assert
    recoverToSucceededIf[Exception](f)
  }

  "startRunningSynchronized" should "pick an configured higher epoch to start running synchronized" in async {
    //Arrange
    val component = constructComponent()
    when(syncStateNode.getData()) thenReturn Future.successful(Some(SynchronizationState(KafkaSourceState.Ready)))
    when(sourceEpochs.getLatestEpochId()) thenReturn Future.successful(2L)

    //Act
    await(component.startRunningSynchronized())

    //Assert
    verify(commandNode, times(1)).push(SourceCommand(KafkaSourceCommand.synchronize, Some("4")))
    assert(true)
  }

  it should "throw an exception if the source was not in ready state" in {
    //Arrange
    val component = constructComponent()
    when(syncStateNode.getData()) thenReturn Future.successful(Some(SynchronizationState(KafkaSourceState.CatchingUp)))
    when(sourceEpochs.getLatestEpochId()) thenReturn Future.successful(2L)

    //Act
    val f = component.startRunningSynchronized()

    //Assert
    recoverToSucceededIf[Exception](f)
  }



  override def beforeEach(): Unit = {
    super.beforeEach()
    sourceNode = mock[QuerySourceNode]
    syncStateNode = mock[SourceSynchronizationStateNode]
    sourceEpochs = mock[SourceEpochCollection]
    commandNode = mock[QuerySourceCommandNode]

    mockLock(sourceNode)

    when(sourceNode.getEpochs()) thenReturn sourceEpochs
    when(sourceNode.getSyncState()) thenReturn syncStateNode
    when(sourceNode.getCommandNode()) thenReturn commandNode


    //Some default values
    when(syncStateNode.getData()) thenReturn Future.successful(Some(SynchronizationState(KafkaSourceState.UnSynchronized)))
    when(syncStateNode.setData(ArgumentMatchers.any())) thenReturn Future.successful(())

    super.beforeEach()
  }

  private def constructComponent(): SourceAlignment = new SourceAlignment(sourceNode)

}
