package org.codefeedr.core.Library.Metastore

import org.codefeedr.core.Library.Internal.Zookeeper.{ZkNode, ZkNodeBase}
import org.codefeedr.Model.Zookeeper.Partition

class ConsumerPartitionNode(name: String, parent: ZkNodeBase)
    extends ZkNode[Partition](name, parent) {}
