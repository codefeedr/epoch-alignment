package org.codefeedr.core.Library.Metastore

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.core.Library.Internal.Zookeeper.{ZkNode, ZkNodeBase, ZkStateNode}
import org.codefeedr.Model.Zookeeper.{QuerySink, QuerySource}

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

class QuerySourceNode(name: String, parent: ZkNodeBase)
    extends ZkNode[QuerySource](name, parent)
    with ZkStateNode[QuerySource, Boolean]
    with LazyLogging {

  def GetConsumers(): ConsumerCollection = new ConsumerCollection("consumers", this)

  override def TypeT(): ClassTag[Boolean] = ClassTag(classOf[Boolean])
  override def InitialState(): Boolean = true

  /**
    * Computes the aggregate state of the subject
    * If the state changed, the change is propagated to the parent.
    * @return
    */
  def UpdateState(): Future[Unit] = async {
    val currentState = await(GetState()).get
    //Only perform update if the source nod was not active.
    if (currentState) {
      val childState = await(GetConsumers().GetState())
      if (!childState) {
        logger.info(
          s"Closing source $name of subject ${parent.Parent().name} because all consumers closed.")
        await(SetState(childState))
      }
    }
  }
}
