package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{
  ZkClientComponent,
  ZkCollectionNode,
  ZkNodeBase
}

class ConsumerPartitionCollection(parent: ZkNodeBase)
    extends ZkCollectionNode[ConsumerPartitionNode,Unit]("partitions",
                                                    parent,
                                                    (n, p) => new ConsumerPartitionNode(n, p)) {
  this: ZkClientComponent =>
}
