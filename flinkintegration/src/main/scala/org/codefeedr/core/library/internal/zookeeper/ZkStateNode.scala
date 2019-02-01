package org.codefeedr.core.library.internal.zookeeper

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global

trait ZkStateNode[TNode, TState] extends ZkNode[TNode] {

  /**
    * Override postcreate to create initial state
    *
    * @return
    */
  def postCreate(): Future[Unit]

  /**
    * Override delete to also delete the state
    *
    * @return a future that resolves when the node has been deleted
    */
  def delete(): Future[Unit]

  /**
    * The base class needs to expose the typeTag, no typeTag constraints can be put on traits
    *
    * @return
    */
  def typeT(): ClassTag[TState]

  /**
    * The initial state of the node. State is not allowed to be empty
    *
    * @return
    */
  def initialState(): TState

  /**
    * Retrieves the state node
    *
    * @return
    */
  def getStateNode(): ZkNode[TState]

  /**
    * Retrieves the state
    *
    * @return
    */
  def getState(): Future[Option[TState]]

  /**
    * Close the current subject
    * TODO: Implement hooks
    *
    * @return
    */
  def setState(state: TState): Future[Unit]

  /**
    * Places a watch on the state that returns when the given condition evaluates to true
    * Uses hooks on zookeeper
    *
    * @param f Watching condition
    * @return
    */
  def watchState(f: TState => Boolean): Future[TState]
}

trait ZkStateNodeComponent extends ZkNodeComponent { this: ZkClientComponent =>

  trait ZkStateNodeImpl[TNode, TState] extends ZkNodeImpl[TNode] with ZkStateNode[TNode, TState] {

    /**
      * Override postcreate to create initial state
      *
      * @return
      */
    override def postCreate(): Future[Unit] = async {
      await(getStateNode().create(initialState()))
    }

    /**
      * Override delete to also delete the state
      *
      * @return a future that resolves when the node has been deleted
      */
    override def delete(): Future[Unit] = async {
      await(getStateNode().delete())
      await(super.delete())
    }

    /**
      * Retrieves the state node
      *
      * @return
      */
    override def getStateNode(): ZkNode[TState] = getChild[TState]("state")(typeT())

    /**
      * Retrieves the state
      *
      * @return
      */
    override def getState(): Future[Option[TState]] = {
      getChild[TState]("state")(typeT()).getData()
    }

    /**
      * Close the current subject
      * TODO: Implement hooks
      *
      * @return
      */
    override def setState(state: TState): Future[Unit] = {
      getChild[TState]("state")(typeT()).setData(state)
    }

    /**
      * Places a watch on the state that returns when the given condition evaluates to true
      * Uses hooks on zookeeper
      *
      * @param f Watching condition
      * @return
      */
    override def watchState(f: TState => Boolean): Future[TState] =
      getChild[TState]("state")(typeT()).awaitCondition(f)
  }

}
