package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.{ZkCollectionNode, ZkNodeBase}

class ConsumerPartitionCollection(parent: ZkNodeBase)
  extends ZkCollectionNode[ConsumerPartitionNode]("partitions", parent, (n, p) => new ConsumerPartitionNode(n,p)){

}
