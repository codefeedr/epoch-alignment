package org.codefeedr.core.Library.Internal.Zookeeper

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global

trait ZkStateNode[TNode, TState] extends ZkNode[TNode] {

  /**
    * Override postcreate to create initial state
    * @return
    */
  override def PostCreate(): Future[Unit] = async {
    await(GetStateNode().Create(InitialState()))
  }

  /**
    * Override delete to also delete the state
    * @return a future that resolves when the node has been deleted
    */
  override def Delete(): Future[Unit] = async {
    await(GetStateNode().Delete())
    await(super.Delete())
  }

  /**
    * The base class needs to expose the typeTag, no typeTag constraints can be put on traits
    * @return
    */
  def TypeT(): ClassTag[TState]

  /**
    * The initial state of the node. State is not allowed to be empty
    * @return
    */
  def InitialState(): TState

  /**
    * Retrieves the state node
    * @return
    */
  def GetStateNode(): ZkNode[TState] = GetChild[TState]("state")(TypeT())

  /**
    * Retrieves the state
    * @return
    */
  def GetState(): Future[Option[TState]] = GetChild[TState]("state")(TypeT()).GetData()

  /**
    * Close the current subject
    * TODO: Implement hooks
    * @return
    */
  def SetState(state: TState): Future[Unit] = GetChild[TState]("state")(TypeT()).SetData(state)

  /**
    * Places a watch on the state that returns when the given condition evaluates to true
    * Uses hooks on zookeeper
    * @param f Watching condition
    * @return
    */
  def WatchState(f: TState => Boolean): Future[TState] =
    GetChild[TState]("state")(TypeT()).AwaitCondition(f)
}
