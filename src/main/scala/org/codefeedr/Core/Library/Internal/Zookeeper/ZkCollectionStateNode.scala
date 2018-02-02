package org.codefeedr.Core.Library.Internal.Zookeeper


import com.typesafe.scalalogging.LazyLogging
import rx.lang.scala.Observable

import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{Future, Promise}

/**
  * Class managing scala collection state
  */
trait ZkCollectionStateNode[
    TChildNode <: ZkStateNode[TChild, TChildState], TChild, TChildState, TAggregateState]
    extends ZkCollectionNode[TChildNode]
  with LazyLogging {
  def GetChildren(): Future[Iterable[TChildNode]]

  /**
    * Initial value of the aggreagate state before the fold
    * @return
    */
  def Initial(): TAggregateState

  /**
    * Mapping from the child to the aggregate state
    * @param child
    * @return
    */
  def MapChild(child: TChildState): TAggregateState

  /**
    * Reduce operator of the aggregation
    * @param left
    * @param right
    * @return
    */
  def ReduceAggregate(left: TAggregateState, right: TAggregateState): TAggregateState



  def GetState(): Future[TAggregateState] = async {
    val consumerNodes = await(GetChildren())
    val states = await(
      Future.sequence(
        consumerNodes.map(o => o.GetStateNode().GetData().map(o => MapChild(o.get))).toList))
    states.foldLeft(Initial())(ReduceAggregate)
  }

  /**
    * Returns a future that resolves when the given condition evaluates to true for all children
    * TODO: Find a better way to implement this
    * @param f condition to evaluate for each child
    * @return
    */
  def WatchStateAggregate(f: TChildState => Boolean): Future[Boolean] = async {
    val p = Promise[Boolean]

    //Accumulator used
    def accumulator(current:List[String],element:(String, Boolean)) = {
      if(element._2) {
        current.filter(o => o != element._1)
      } else {
        if(!current.contains(element._1)) {
          current ++ List[String](element._1)
        } else {
          current
        }
      }
    }

    //First obtain the initial state
    val childNodes = await(GetChildren())
    val initialState = await(
        Future.sequence(
          childNodes.map(
            child => child.GetStateNode().GetData().map(state => (child.name, !f(state.get)))).toList))
        .filter(o => o._2)
      .map(o => o._1)

    logger.debug(s"initial state: $initialState")

    //Once the initial state of all children is obtained, start watching
    val subscription = ObserveNewChildren()
        .flatMap(o => o.GetStateNode().ObserveData()
            .map(state => (o.name, f(state))) ++ Observable.just((o.name, true)))
            .map(o => {logger.debug(s"got event ${o}");o})
        .scan[List[String]](initialState)(accumulator)
      .map(o =>{logger.debug(s"Current state: $o");o})
      .map(o => o.isEmpty)
      .map(o =>{logger.debug(s"Current state after throttle: $o");o})
      .subscribe(o => if(o) p.success(true)
      ,error => p.failure(error),
        () => p.success(false))

      //unsubscribe on comlete if needed
      p.future.onComplete(o => if(o.get) subscription.unsubscribe())
      await(p.future)
  }
}
