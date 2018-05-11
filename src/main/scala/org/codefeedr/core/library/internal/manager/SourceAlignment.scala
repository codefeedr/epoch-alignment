package org.codefeedr.core.library.internal.manager

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.core.library.internal.kafka.source.KafkaSourceState
import org.codefeedr.core.library.metastore.sourcecommand.{KafkaSourceCommand, SourceCommand}
import org.codefeedr.core.library.metastore.{QuerySourceNode, SynchronizationState}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}
import scala.concurrent.Future

/**
  * Class providing logic relevant for the alignment of a source
  */
class SourceAlignment(sourceNode: QuerySourceNode) extends LazyLogging {

  /** SyncState node of the source*/
  private lazy val syncState = sourceNode.getSyncState()
  private lazy val commandNode = sourceNode.getCommandNode()

  /**
    * Starts the alignment
    * Uses locking to prevent concurrency issues
    * @return a future that succeeds when the allignment process started, or fails when it failed to start
    */
  def startAlignment(): Future[Unit] =
    sourceNode.asyncWriteLock(() =>
      async {
        val state = await(sourceNode.getSyncState().getData()).get.state
        state match {
          case KafkaSourceState.UnSynchronized => await(triggerStartAlignment())
          case _ => throw new Exception(s"Cannot start alignment when source is in state $state")
        }
    })

  /** Performs the operations to start synchronization. Does not perform any checks*/
  private def triggerStartAlignment(): Future[Unit] = async {
    await(syncState.setData(SynchronizationState(KafkaSourceState.CatchingUp)))
    commandNode.push(SourceCommand(KafkaSourceCommand.startSynchronize))
  }

}
