package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper._
import org.codefeedr.model.zookeeper.Consumer

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

case class ConsumerState(finalEpoch: Option[Long], open: Boolean) {
  def aggregate(that: ConsumerState) = ConsumerState(
    finalEpoch match {
      case Some(t) =>
        that.finalEpoch match {
          case Some(o) => Some(Math.max(o, t))
          case _ => None
        }
      case None => None
    },
    open || that.open
  )
}

trait ConsumerNode extends ZkStateNode[Consumer, ConsumerState] {

  def postCreate(): Future[Unit]

  def getSyncState(): SourceSynchronizationStateNode

  def consumerCollection(): ConsumerCollection

  def typeT(): ClassTag[ConsumerState]

  /**
    * The initial state of the node. State is not allowed to be empty
    *
    * @return
    */
  def initialState(): ConsumerState

  def setState(state: ConsumerState): Future[Unit]
}

trait ConsumerNodeComponent extends ZkStateNodeComponent {
  this: ZkClientComponent with SourceSynchronizationStateNodeComponent =>

  class ConsumerNodeImpl(name: String, parent: ZkNodeBase)
      extends ZkNodeImpl[Consumer](name, parent)
      with ZkStateNodeImpl[Consumer, ConsumerState]
      with ConsumerNode {
    override def postCreate(): Future[Unit] =
      for {
        _ <- super.postCreate()
        _ <- getSyncState().create()
      } yield {}

    override def getSyncState(): SourceSynchronizationStateNode =
      new SourceSynchronizationStateNodeImpl(this)

    override def consumerCollection(): ConsumerCollection =
      parent().asInstanceOf[ConsumerCollection]

    override def typeT(): ClassTag[ConsumerState] = ClassTag(classOf[ConsumerState])

    /**
      * The initial state of the node. State is not allowed to be empty
      *
      * @return
      */
    override def initialState(): ConsumerState =
      ConsumerState(None, open = true)

    override def setState(state: ConsumerState): Future[Unit] = async {
      await(super.setState(state))
      //Call update on query source state, because this consumer state change might impact it
      await(parent.parent().asInstanceOf[QuerySourceNode].updateState())
    }
  }

}
