package org.codefeedr.Core.Library.Internal.Zookeeper

import scala.async.Async.{async,await}
import scala.concurrent.Future

/**
  * Class managing scala collection state
  */
trait ZkCollectionState[TChildNode <: StateNode[TChildNode]] extends ZkNodeBase {
  def GetChildren(): Future[Iterable[TChildNode]]

  def GetState(): Future[Boolean] = async {
    val consumerNodes = await(GetChildren())
    val states = await(Future.sequence(consumerNodes.map(o => o.GetState().GetData().map(o => o.get)).toList))
    states.foldLeft(false)(_ || _)
  }


  /**
    * Returns a future that resolves when the given condition evaluates to true for all children
    * @param f condition to evaluate for each child
    * @return
    */
  def WatchStateAggregate(f: Boolean => Boolean): Future[Unit] = {


  }
}
