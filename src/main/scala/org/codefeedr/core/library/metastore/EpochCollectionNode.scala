package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkCollectionNode, ZkNodeBase}

/**
  * Epoch collection node
  * Contains epoch mappings to offset pet job
  * @param parent
  */
class EpochCollectionNode(parent: ZkNodeBase)
    extends ZkCollectionNode[EpochNode]("epochs", parent, (n, p) => new EpochNode(n.toInt, p)) {}
