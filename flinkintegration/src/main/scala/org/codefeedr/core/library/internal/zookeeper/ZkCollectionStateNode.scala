package org.codefeedr.core.library.internal.zookeeper

import com.typesafe.scalalogging.LazyLogging
import rx.lang.scala.{Observable, Subscription}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}
import scala.collection.mutable
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{Future, Promise}

/**
  * Class managing scala collection state
  */
trait ZkCollectionStateNode[
    TChildNode <: ZkStateNode[TChild, TChildState], TData, TChild, TChildState, TAggregateState]
    extends ZkCollectionNode[TChildNode, TData]
    with LazyLogging {
  def getChildren(): Future[Iterable[TChildNode]]

  /**
    * Initial value of the aggreagate state before the fold
    * @return
    */
  def initial(): TAggregateState

  /**
    * Mapping from the child to the aggregate state
    * @param child
    * @return
    */
  def mapChild(child: TChildState): TAggregateState

  /**
    * Reduce operator of the aggregation
    * @param left
    * @param right
    * @return
    */
  def reduceAggregate(left: TAggregateState, right: TAggregateState): TAggregateState

  def getState(): Future[TAggregateState] = async {
    val consumerNodes = await(getChildren())
    val states = await(
      Future.sequence(
        consumerNodes.map(o => o.getStateNode().getData().map(o => mapChild(o.get))).toList))
    states.foldLeft(initial())(reduceAggregate)
  }

  /**
    * Returns a future that resolves when the given condition evaluates to true for all children
    * TODO: Find a better way to implement this
    * @param f condition to evaluate for each child
    * @return
    */
  def watchStateAggregate(f: TChildState => Boolean): Future[Boolean] = {
    val aggregateWatch = new AggregateWatch[TChildNode, TChild, TChildState](f)
    //Observe the children
    observeNewChildren().subscribe(o => aggregateWatch.newChild(o))
    aggregateWatch.future
  }
}

/**
  * Class handling the state needed to watch node aggregates
  * @param f mapping method from the state
  * @tparam TChildNode Type of the childNode
  * @tparam TChild Type of the child
  * @tparam TChildState Type of the childs state
  */
private class AggregateWatch[TChildNode <: ZkStateNode[TChild, TChildState], TChild, TChildState](
    f: TChildState => Boolean) {
  private val lock = new Object()
  private val p = Promise[Boolean]
  //Contains the current state

  private val stateMap = mutable.Map[String, Boolean]()
  private val subscriptionMap = mutable.Map[String, Subscription]()

  /**
    * Retrieve the future of the aggregate watch
    *
    * @return
    */
  def future: Future[Boolean] = p.future

  /**
    * Methods adds a new child to the collection
    * Synchronized method
    *
    * @param childNode
    */
  def newChild(childNode: TChildNode): Unit = lock.synchronized {
    stateMap += childNode.name -> false
    val subscription = childNode
      .getStateNode()
      .observeData()
      .map(s => f(s))
      .subscribe(o =>
                   synchronized {
                     stateMap.update(childNode.name, o)
                     if (o) {
                       checkFinish()
                     }
                 },
                 error => {
                   p.failure(error)
                   cleanup()
                 },
                 () => {
                   remove(childNode.name)
                   checkFinish()
                 })
    subscriptionMap += childNode.name -> subscription
  }

  private def checkFinish(): Unit = lock.synchronized {
    if (stateMap.forall(p => p._2)) {
      if (!p.isCompleted) {
        p.success(true)
        cleanup()
      }
    }
  }

  /*
      Removes a child from the internal state
   */
  private def remove(child: String) = {
    stateMap.remove(child)
    if (!subscriptionMap(child).isUnsubscribed) {
      subscriptionMap(child).unsubscribe()
    }
    subscriptionMap.remove(child)
  }

  /**
    * Cleans up all placed subscriptions
    */
  private def cleanup(): Unit = {
    val children = stateMap.keys.toList
    children.foreach(remove)
  }
}
