package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper._
import org.codefeedr.model.zookeeper.Consumer

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

trait ConsumerNode extends ZkStateNode[Consumer, Boolean] {

  def postCreate(): Future[Unit]

  def getSyncState(): SourceSynchronizationStateNode

  def consumerCollection(): ConsumerCollection

  def typeT(): ClassTag[Boolean]

  /**
    * The initial state of the node. State is not allowed to be empty
    *
    * @return
    */
  def initialState(): Boolean

  def setState(state: Boolean): Future[Unit]
}


trait ConsumerNodeComponent extends ZkStateNodeComponent {
  this:ZkClientComponent
  with SourceSynchronizationStateNodeComponent =>

  class ConsumerNodeImpl(name: String, parent: ZkNodeBase)
    extends ZkNodeImpl[Consumer](name, parent)
      with ZkStateNodeImpl[Consumer, Boolean] with ConsumerNode {
    override def postCreate(): Future[Unit] =
      for {
        _ <- super.postCreate()
        _ <- getSyncState().create()
      } yield {}

    override def getSyncState(): SourceSynchronizationStateNode = new SourceSynchronizationStateNodeImpl(this)

    override def consumerCollection(): ConsumerCollection = parent().asInstanceOf[ConsumerCollection]

    override def typeT(): ClassTag[Boolean] = ClassTag(classOf[Boolean])

    /**
      * The initial state of the node. State is not allowed to be empty
      *
      * @return
      */
    override def initialState(): Boolean = true

    override def setState(state: Boolean): Future[Unit] = async {
      await(super.setState(state))
      //Call update on query source state, because this consumer state change might impact it
      await(parent.parent().asInstanceOf[QuerySourceNode].updateState())
    }
  }

}