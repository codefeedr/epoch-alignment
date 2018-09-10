package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper._
import org.codefeedr.model.zookeeper.Partition

trait PartitionNode extends ZkNode[Partition]

trait PartitionNodeComponent extends ZkNodeComponent { this: ZkClientComponent =>

  class PartitionNodeImpl(val zk: ZkClient)(partitionNr: Int, parent: ZkNodeBase)
      extends ZkNodeImpl[Partition](s"$partitionNr", parent)
      with PartitionNode {}

}
