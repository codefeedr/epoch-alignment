package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkCollectionNode, ZkNodeBase}

class ConsumerPartitionCollection(parent: ZkNodeBase)
    extends ZkCollectionNode[ConsumerPartitionNode]("partitions",
                                                    parent,
                                                    (n, p) => new ConsumerPartitionNode(n, p)) {}
