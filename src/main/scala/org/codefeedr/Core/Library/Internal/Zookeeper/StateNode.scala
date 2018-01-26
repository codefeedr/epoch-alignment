package org.codefeedr.Core.Library.Internal.Zookeeper

import scala.concurrent.Future

trait StateNode[T] extends ZkNode[T]{

  /**
    * Retrieves the state of the consumer, checks if the consumer is still open
    * TODO: This will be refactored to use a generic type as state instead of just a boolean
    * @return
    */
  def GetState(): ZkNode[Boolean] = GetChild[Boolean]("state")

  /**
    * Close the current subject
    * TODO: Implement hooks
    * @return
    */
  def SetState(state: Boolean) = GetChild[Boolean]("state").SetData(state)

  /**
    * Places a watch on the state that returns when the given condition evaluates to true
    * Uses hooks on zookeeper
    * @param f Watching condition
    * @return
    */
  def WatchState(f: Boolean => Boolean) : Future[Boolean] = GetChild[Boolean]("state").AwaitCondition(f)
}
