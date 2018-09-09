package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkClient, ZkClientComponent, ZkNode, ZkNodeBase}
import org.codefeedr.model.zookeeper.Partition

class ConsumerPartitionNode(name: String, parent: ZkNodeBase)
                           (implicit override val zkClient: ZkClient)
    extends ZkNode[Partition](name, parent) {}
