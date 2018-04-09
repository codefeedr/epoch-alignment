package org.codefeedr.core.library.metastore
import org.codefeedr.core.library.internal.zookeeper.{ZkCollectionNode, ZkNodeBase}

class SourceEpochCollection(parent: ZkNodeBase)
    extends ZkCollectionNode[SourceEpochNode]("epochs",
                                              parent,
                                              (n, p) => new SourceEpochNode(n.toInt, p)) {}
