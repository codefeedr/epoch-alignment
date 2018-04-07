package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkCollectionNode, ZkNodeBase}

class EpochPartitionCollection(parent: ZkNodeBase)
    extends ZkCollectionNode[EpochPartition]("partitions",
                                             parent,
                                             (n, p) => new EpochPartition(n, p)) {}
