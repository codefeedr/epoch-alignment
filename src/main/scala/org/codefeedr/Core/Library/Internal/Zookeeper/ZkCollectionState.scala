package org.codefeedr.Core.Library.Internal.Zookeeper

import rx.lang.scala.Observable
import scala.concurrent.ExecutionContext.Implicits.global

import scala.async.Async.{async, await}
import scala.concurrent.{Future, Promise}

/**
  * Class managing scala collection state
  */
trait ZkCollectionState[TChildNode <: StateNode[TChild],TChild] extends ZkCollectionNode[TChildNode] {
  def GetChildren(): Future[Iterable[TChildNode]]

  def GetState(): Future[Boolean] = async {
    val consumerNodes = await(GetChildren())
    val states = await(Future.sequence(consumerNodes.map(o => o.GetState().GetData().map(o => o.get)).toList))
    states.foldLeft(false)(_ || _)
  }


  /**
    * Returns a future that resolves when the given condition evaluates to true for all children
    * TODO: Find a better way to implement this
    * @param f condition to evaluate for each child
    * @return
    */
  def WatchStateAggregate(f: Boolean => Boolean): Future[Unit] = {
    val p = Promise[Unit]
    var i: Int = 0
    val s = ObserveNewChildren().map(o => o.WatchState(f))
      .subscribe(future => {
        i += 1
        future.onSuccess {case _ =>
          i -= 1
          if(i ==0) {
            p.success()
          }
        }
      })
    //
    p.future.onComplete(_ => s.unsubscribe())
    p.future
  }

}
