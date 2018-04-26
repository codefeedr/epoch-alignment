package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{
  ZkCollectionNode,
  ZkCollectionStateNode,
  ZkNodeBase
}
import org.codefeedr.model.zookeeper.Partition

class EpochPartitionCollection(parent: ZkNodeBase)
    extends ZkCollectionNode[EpochPartition,Unit]("partitions",
                                             parent,
                                             (n, p) => new EpochPartition(n, p))
    with ZkCollectionStateNode[EpochPartition,Unit, Partition, Boolean, Boolean] {

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
