package org.codefeedr.core.library.metastore

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.core.library.internal.zookeeper.{ZkNode, ZkNodeBase, ZkStateNode}
import org.codefeedr.model.zookeeper.{Producer, QuerySink}

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

class QuerySinkNode(name: String, parent: ZkNodeBase)
    extends ZkNode[QuerySink](name, parent)
    with ZkStateNode[QuerySink, Boolean]
    with LazyLogging {

  def getProducers(): ProducerCollection = new ProducerCollection("producers", this)

  def getEpochs(): EpochCollectionNode = new EpochCollectionNode(this)

  override def typeT(): ClassTag[Boolean] = ClassTag(classOf[Boolean])
  override def initialState(): Boolean = true

  override def setState(state: Boolean): Future[Unit] = async {
    await(super.setState(state))
    //Call subjectNode to update, because the state of the sink might influence the subjects node
    await(parent.parent().asInstanceOf[SubjectNode].updateState())
  }

  /**
    * Computes the aggregate state of the subject
    * If the state changed, the change is propagated to the parent.
    * @return
    */
  def updateState(): Future[Unit] = async {
    val currentState = await(getState()).get
    //Only perform update if the source nod was not active.
    if (currentState) {
      val childState = await(getProducers().getState())
      await(setState(childState))
    }
  }
}
