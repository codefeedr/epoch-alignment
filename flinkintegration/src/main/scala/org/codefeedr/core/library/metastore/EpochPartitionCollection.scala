package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper._
import org.codefeedr.model.zookeeper.Partition


trait EpochPartitionCollection extends ZkCollectionStateNode[EpochPartition, Unit, Partition, Boolean, Boolean]

trait EpochPartitionCollectionComponent extends ZkCollectionStateNodeComponent {
  this:ZkClientComponent
  with EpochPartitionComponent
  =>

  class EpochPartitionCollectionImpl(parent: ZkNodeBase)
    extends ZkCollectionNodeImpl[EpochPartition, Unit]("partitions",
      parent,
      (n, p) => new EpochPartitionImpl(n, p))
      with ZkCollectionStateNodeImpl[EpochPartition, Unit, Partition, Boolean, Boolean]
    with EpochPartitionCollection {

    /**
      * Initial value of the aggreagate state before the fold
      *
      * @return
      */
    override def initial(): Boolean = true

    /**
      * Mapping from the child to the aggregate state
      *
      * @param child
      * @return
      */
    override def mapChild(child: Boolean): Boolean = child

    /**
      * Reduce operator of the aggregation
      *
      * @param left
      * @param right
      * @return
      */
    override def reduceAggregate(left: Boolean, right: Boolean): Boolean = left && right
  }

}