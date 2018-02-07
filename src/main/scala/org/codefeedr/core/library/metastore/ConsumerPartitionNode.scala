package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkNode, ZkNodeBase}
import org.codefeedr.Model.zookeeper.Partition

class ConsumerPartitionNode(name: String, parent: ZkNodeBase)
    extends ZkNode[Partition](name, parent) {}
