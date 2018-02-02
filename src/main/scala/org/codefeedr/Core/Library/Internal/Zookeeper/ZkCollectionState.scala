package org.codefeedr.Core.Library.Internal.Zookeeper

import rx.lang.scala.Observable
import scala.concurrent.ExecutionContext.Implicits.global

import scala.async.Async.{async, await}
import scala.concurrent.{Future, Promise}

/**
  * Class managing scala collection state
  */
trait ZkCollectionState[
    TChildNode <: ZkStateNode[TChild, TChildState], TChild, TChildState, TAggregateState]
    extends ZkCollectionNode[TChildNode] {
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
  def WatchStateAggregate(f: TChildState => Boolean): Future[Unit] = {
    val p = Promise[Unit]
    var i: Int = 0
    val s = ObserveNewChildren()
      .map(o => o.WatchState(f))
      .subscribe(future => {
        i += 1
        future.onSuccess {
          case _ =>
            i -= 1
            if (i == 0) {
              p.success()
            }
        }
      })
    //
    p.future.onComplete(_ => s.unsubscribe())
    p.future
  }

}
