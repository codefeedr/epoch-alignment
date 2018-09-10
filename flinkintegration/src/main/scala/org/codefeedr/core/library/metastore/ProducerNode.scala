package org.codefeedr.core.library.metastore

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.core.library.internal.zookeeper._
import org.codefeedr.model.zookeeper.{Consumer, Producer}

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

trait ProducerNode extends ZkStateNode[Producer, Boolean] {

  def typeT(): ClassTag[Boolean]

  def initialState(): Boolean

  def setState(state: Boolean): Future[Unit]
}


trait ProducerNodeComponent extends ZkStateNodeComponent {
  this:ZkClientComponent =>

  class ProducerNodeImpl(name: String, parent: ZkNodeBase)
    extends ZkNodeImpl[Producer](name, parent)
      with ZkStateNodeImpl[Producer, Boolean]
      with LazyLogging with ProducerNode {

    override def typeT(): ClassTag[Boolean] = ClassTag(classOf[Boolean])

    override def initialState(): Boolean = true

    override def setState(state: Boolean): Future[Unit] = async {
      logger.debug(s"Setting producer state of $name to $state")
      await(super.setState(state))
      await(parent.parent().asInstanceOf[QuerySinkNode].updateState())
    }

  }

}