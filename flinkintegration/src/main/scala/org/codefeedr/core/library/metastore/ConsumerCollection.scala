package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper._
import org.codefeedr.model.zookeeper.Consumer

import scala.concurrent.Future

trait ConsumerCollection
    extends ZkCollectionNode[ConsumerNode, Unit]
    with ZkCollectionStateNode[ConsumerNode, Unit, Consumer, ConsumerState, ConsumerState] {
  def querySource(): QuerySourceNode = parent().asInstanceOf[QuerySourceNode]
}

trait ConsumerCollectionComponent extends ZkCollectionStateNodeComponent {
  this: ZkClientComponent with ConsumerNodeComponent =>

  class ConsumerCollectionImpl(subjectName: String, parent: ZkNodeBase)
      extends ZkCollectionNodeImpl[ConsumerNode, Unit](
        "consumers",
        parent,
        (name, parent) => new ConsumerNodeImpl(name, parent))
      with ZkCollectionStateNodeImpl[ConsumerNode, Unit, Consumer, ConsumerState, ConsumerState]
      with ConsumerCollection {

    override def initial(): ConsumerState = ConsumerState(Some(-1), false)

    override def mapChild(child: ConsumerState): ConsumerState = child

    override def reduceAggregate(left: ConsumerState, right: ConsumerState): ConsumerState =
      left.aggregate(right)

    /**
      * Returns a future that resolves when the given condition evaluates to true for all children
      * TODO: Find a better way to implement this
      *
      * @param f condition to evaluate for each child
      * @return
      */
    override def watchStateAggregate(f: ConsumerState => Boolean): Future[Boolean] = ???
  }

}
