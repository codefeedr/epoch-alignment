package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkClient, ZkClientComponent, ZkCollectionNode, ZkNodeBase}

class ConsumerPartitionCollection(parent: ZkNodeBase)
                                 (implicit override val zkClient: ZkClient)
    extends ZkCollectionNode[ConsumerPartitionNode, Unit](
      "partitions",
      parent,
      (n, p) => new ConsumerPartitionNode(n, p)) { this: ZkClientComponent =>
}
