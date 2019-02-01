package org.codefeedr.core.library.metastore
import org.codefeedr.core.library.internal.zookeeper.{
  ZkClientComponent,
  ZkNodeBase,
  ZkStateNode,
  ZkStateNodeComponent
}

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

case class JobConsumerState(finalEpoch: Option[Long], open: Boolean) {
  def aggregate(that: JobConsumerState) = JobConsumerState(
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

trait JobConsumerNode extends ZkStateNode[Unit, JobConsumerState] {}

trait JobConsumerNodeComponent extends ZkStateNodeComponent {
  this: ZkClientComponent
    with ConsumerCollectionComponent
    with SourceEpochCollectionComponent
    with SourceSynchronizationStateNodeComponent
    with QuerySourceCommandNodeComponent =>

  class JobConsumerNodeImpl(name: String, parent: ZkNodeBase)
      extends ZkNodeImpl[Unit](name, parent)
      with ZkStateNodeImpl[Unit, JobConsumerState]
      with JobConsumerNode {

    /**
      * The base class needs to expose the typeTag, no typeTag constraints can be put on traits
      *
      * @return
      */
    override def typeT(): ClassTag[JobConsumerState] = ClassTag(classOf[JobConsumerState])

    /**
      * The initial state of the node. State is not allowed to be empty
      *
      * @return
      */
    override def initialState(): JobConsumerState = JobConsumerState(None, true)

    override def postCreate(): Future[Unit] =
      for {
        _ <- super.postCreate()
      } yield {}

    override def setState(state: JobConsumerState): Future[Unit] = async {
      await(super.setState(state))
    }
  }
}
