package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper._

trait ConsumerPartitionCollection extends ZkCollectionNode[ConsumerPartitionNode,Unit]

trait ConsumerPartitionCollectionComponent extends ZkCollectionNodeComponent {
      this: ZkClientComponent
      with ConsumerPartitionComponent =>

      class ConsumerPartitionCollectionImpl(parent: ZkNodeBase)
        extends ZkCollectionNodeImpl[ConsumerPartitionNode, Unit](
          "partitions",
          parent,
          (n, p) => new ConsumerPartitionNodeImpl(n, p))
        with ConsumerPartitionCollection {
      }
}