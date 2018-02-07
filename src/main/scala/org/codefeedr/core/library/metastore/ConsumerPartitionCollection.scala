package org.codefeedr.core.Library.Metastore

import org.codefeedr.core.Library.Internal.Zookeeper.{ZkCollectionNode, ZkNodeBase}

class ConsumerPartitionCollection(parent: ZkNodeBase)
    extends ZkCollectionNode[ConsumerPartitionNode]("partitions",
                                                    parent,
                                                    (n, p) => new ConsumerPartitionNode(n, p)) {}
