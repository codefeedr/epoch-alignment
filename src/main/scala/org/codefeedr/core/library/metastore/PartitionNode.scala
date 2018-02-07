package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkClient, ZkNode, ZkNodeBase}
import org.codefeedr.model.zookeeper.Partition

class PartitionNode(val zk: ZkClient)(partitionNr: Int, parent: ZkNodeBase)
    extends ZkNode[Partition](s"$partitionNr", parent) {}
