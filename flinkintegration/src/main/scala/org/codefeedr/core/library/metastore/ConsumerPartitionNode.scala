package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper._
import org.codefeedr.model.zookeeper.Partition

trait ConsumerPartitionNode extends ZkNode[Partition]

trait ConsumerPartitionComponent extends ZkNodeComponent { this: ZkClientComponent =>
  class ConsumerPartitionNodeImpl(name: String, parent: ZkNodeBase)
      extends ZkNodeImpl[Partition](name, parent)
      with ConsumerPartitionNode {}
}
