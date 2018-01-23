package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.{ZkCollectionNode, ZkNodeBase}

class ConsumerPartitionCollection(name: String, parent: ZkNodeBase)
  extends ZkCollectionNode[ConsumerPartitionNode](name, parent, (n, p) => new ConsumerPartitionNode(n,p)){

}
