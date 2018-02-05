package org.codefeedr.Core.Library.Metastore

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.Core.Library.Internal.Zookeeper.{ZkNode, ZkNodeBase, ZkStateNode}
import org.codefeedr.Model.Zookeeper.{Producer, QuerySink}

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

class QuerySinkNode(name: String, parent: ZkNodeBase)
    extends ZkNode[QuerySink](name, parent)
    with ZkStateNode[QuerySink, Boolean]
    with LazyLogging {

  def GetProducers(): ProducerCollection = new ProducerCollection("producers", this)

  override def TypeT(): ClassTag[Boolean] = ClassTag(classOf[Boolean])
  override def InitialState(): Boolean = true

  override def SetState(state: Boolean): Future[Unit] = async {
    await(super.SetState(state))
    //Call subjectNode to update, because the state of the sink might influence the subjects node
    await(parent.Parent().asInstanceOf[SubjectNode].UpdateState())
  }

  /**
    * Computes the aggregate state of the subject
    * If the state changed, the change is propagated to the parent.
    * @return
    */
  def UpdateState(): Future[Unit] = async {
    val currentState = await(GetState()).get
    //Only perform update if the source nod was not active.
    if (currentState) {
      val childState = await(GetProducers().GetState())
      await(SetState(childState))
    }
  }
}
