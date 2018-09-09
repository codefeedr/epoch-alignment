package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.kafka.meta.SourceEpoch
import org.codefeedr.core.library.internal.zookeeper.{ZkClient, ZkNode, ZkNodeBase}

/**
  * Node describing the epoch of a source of a job
  * @param epoch The epoch the node describes
  * @param parent Parent of the node
  */
class SourceEpochNode(epoch: Int, parent: ZkNodeBase)(implicit override val zkClient: ZkClient)
    extends ZkNode[SourceEpoch](s"$epoch", parent) {}
