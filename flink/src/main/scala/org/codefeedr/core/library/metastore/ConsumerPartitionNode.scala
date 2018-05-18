package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkClientComponent, ZkNode, ZkNodeBase}
import org.codefeedr.model.zookeeper.Partition

class ConsumerPartitionNode(name: String, parent: ZkNodeBase)
    extends ZkNode[Partition](name, parent) {}
