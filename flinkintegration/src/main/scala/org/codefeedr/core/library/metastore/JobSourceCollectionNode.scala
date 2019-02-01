package org.codefeedr.core.library.metastore
import org.codefeedr.core.library.internal.zookeeper._

import scala.concurrent.Future

trait JobConsumerCollectionNode
    extends ZkCollectionStateNode[JobConsumerNode, Unit, Unit, JobConsumerState, JobConsumerState] {
  def job(): JobNode = parent().asInstanceOf[JobNode]

}

trait JobConsumerCollectionComponent extends ZkCollectionStateNodeComponent {
  this: ZkClientComponent with JobConsumerNodeComponent =>

  class JobConsumerCollectionNodeImpl(subjectName: String, parent: ZkNodeBase)
      extends ZkCollectionNodeImpl[JobConsumerNode, Unit](
        "jobconsumers",
        parent,
        (name, parent) => new JobConsumerNodeImpl(name, parent))
      with ZkCollectionStateNodeImpl[JobConsumerNode,
                                     Unit,
                                     Unit,
                                     JobConsumerState,
                                     JobConsumerState]
      with JobConsumerCollectionNode {

    override def initial(): JobConsumerState = JobConsumerState(Some(-1), open = false)

    override def mapChild(child: JobConsumerState): JobConsumerState = child

    override def reduceAggregate(left: JobConsumerState,
                                 right: JobConsumerState): JobConsumerState =
      left.aggregate(right)

    /**
      * Returns a future that resolves when the given condition evaluates to true for all children
      * TODO: Find a better way to implement this
      *
      * @param f condition to evaluate for each child
      * @return
      */
    override def watchStateAggregate(f: JobConsumerState => Boolean): Future[Boolean] = ???

  }

}
