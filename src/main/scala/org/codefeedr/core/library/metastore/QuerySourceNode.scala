package org.codefeedr.core.library.metastore

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.core.library.internal.zookeeper.{ZkNode, ZkNodeBase, ZkStateNode}
import org.codefeedr.model.zookeeper.{QuerySink, QuerySource}

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

class QuerySourceNode(name: String, parent: ZkNodeBase)
    extends ZkNode[QuerySource](name, parent)
    with ZkStateNode[QuerySource, Boolean]
    with LazyLogging {

  def getConsumers(): ConsumerCollection = new ConsumerCollection("consumers", this)

  /**
    * Retrieve the epochs of the source
    * @return
    */
  def getEpochs(): SourceEpochCollection = new SourceEpochCollection(this)

  override def typeT(): ClassTag[Boolean] = ClassTag(classOf[Boolean])
  override def initialState(): Boolean = true

  /**
    * Computes the aggregate state of the subject
    * If the state changed, the change is propagated to the parent.
    * @return
    */
  def updateState(): Future[Unit] = async {
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
