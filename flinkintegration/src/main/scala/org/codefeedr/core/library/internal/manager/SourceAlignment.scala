package org.codefeedr.core.library.internal.manager

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.core.library.ConfigFactoryComponent
import org.codefeedr.core.library.internal.kafka.source.KafkaSourceState
import org.codefeedr.core.library.metastore.sourcecommand.{KafkaSourceCommand, SourceCommand}
import org.codefeedr.core.library.metastore.{QuerySourceNode, SynchronizationState}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}
import scala.concurrent.Future

/**
  * Class providing logic relevant for the alignment of a source
  */
class SourceAlignment(sourceNode: QuerySourceNode, configFactory: ConfigFactoryComponent)
    extends LazyLogging {

  /** SyncState node of the source*/
  private lazy val syncState = sourceNode.getSyncState()
  private lazy val commandNode = sourceNode.getCommandNode()
  private lazy val synchronizeAfter: Int =
    configFactory.conf.getInt("codefeedr.synchronization.synchronizeAfter")

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
          case _ =>
            throw new Exception(
              s"Cannot start alignment when source is in state $state. Source has to be in unsynchronized state")
        }
    })

  /** Performs the operations to start synchronization. Does not perform any checks*/
  private def triggerStartAlignment(): Future[Unit] = async {
    await(syncState.setData(SynchronizationState(KafkaSourceState.CatchingUp)))
    commandNode.push(SourceCommand(KafkaSourceCommand.catchUp, None))
  }

  /**
    * Can be called when all consumers of a source are ready
    * Picks a future epoch to synchronize on
    * @return
    */
  def startRunningSynchronized(): Future[Unit] = {
    sourceNode.asyncWriteLock(() =>
      async {
        val state = await(sourceNode.getSyncState().getData()).get.state
        state match {
          case KafkaSourceState.Ready => await(triggerStartSynchronized())
          case _ =>
            throw new Exception(
              s"Cannot start running synchronized when source is in state $state. Source has to be in ready state")
        }

    })
  }

  /**
    * Performs logic required for the sources to start running synchronized
    * @return A future with the source epoch on which the job job will synchronize
    */
  private def triggerStartSynchronized(): Future[Long] = async {
    val syncEpoch = await(sourceNode.getEpochs().getLatestEpochId()) + synchronizeAfter
    val command = SourceCommand(KafkaSourceCommand.synchronize, Some(syncEpoch.toString))
    commandNode.push(command)
    syncEpoch
  }

  /**
    * Creates a future that returns true when all sources are synchronized,
    * or false when the passed epoch has completed.
    * This method sould be called with the synchronization epoch in most cases
    * @return The constructed future
    */
  def whenSynchronized(epoch: Long): Future[Boolean] = ???

}
