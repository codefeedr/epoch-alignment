package org.codefeedr.core.library.metastore

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.core.library.internal.zookeeper._
import org.codefeedr.model.zookeeper.{QuerySink, QuerySource}

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

trait QuerySourceNode extends ZkStateNode[QuerySource, Boolean] {

  def getConsumers(): ConsumerCollection

  /**
    * Retrieve the epochs of the source
    *
    * @return
    */
  def getEpochs(): SourceEpochCollection

  def getSyncState(): SourceSynchronizationStateNode

  /** Retrieve the node that can be used to give instructions to this node */
  def getCommandNode(): QuerySourceCommandNode

  def postCreate(): Future[Unit]

  def typeT(): ClassTag[Boolean]

  def initialState(): Boolean

  /**
    * Computes the aggregate state of the subject
    * If the state changed, the change is propagated to the parent.
    *
    * @return
    */
  def updateState(): Future[Unit]
}

trait QuerySourceNodeComponent extends ZkStateNodeComponent {
  this: ZkClientComponent
    with ConsumerCollectionComponent
    with SourceEpochCollectionComponent
    with SourceSynchronizationStateNodeComponent
    with QuerySourceCommandNodeComponent =>

  /** Creating a querySourceNode will create its "skeleton" */
  class QuerySourceNodeImpl(name: String, parent: ZkNodeBase)
      extends ZkNodeImpl[QuerySource](name, parent)
      with ZkStateNodeImpl[QuerySource, Boolean]
      with LazyLogging
      with QuerySourceNode {

    override def getConsumers(): ConsumerCollection = new ConsumerCollectionImpl("consumers", this)

    /**
      * Retrieve the epochs of the source
      *
      * @return
      */
    override def getEpochs(): SourceEpochCollection = new SourceEpochCollectionImpl(this)

    override def getSyncState(): SourceSynchronizationStateNode =
      new SourceSynchronizationStateNodeImpl(this)

    /** Retrieve the node that can be used to give instructions to this node */
    override def getCommandNode(): QuerySourceCommandNode = new QuerySourceCommandNodeImpl(this)

    override def postCreate(): Future[Unit] = {
      for {
        _ <- super.postCreate()
        _ <- getEpochs().create()
        _ <- getSyncState().create()
        _ <- getCommandNode().create()
        _ <- getConsumers().create()
      } yield {}
    }

    override def typeT(): ClassTag[Boolean] = ClassTag(classOf[Boolean])

    override def initialState(): Boolean = true

    /**
      * Computes the aggregate state of the subject
      * If the state changed, the change is propagated to the parent.
      *
      * @return
      */
    override def updateState(): Future[Unit] = async {
      val currentState = await(getState()).get
      //Only perform update if the source nod was not active.
      if (currentState) {
        val childState = await(getConsumers().getState())
        if (!childState) {
          logger.info(
            s"Closing source $name of subject ${parent.parent().name} because all consumers closed.")
          await(setState(childState))
        }
      }
    }
  }

}
