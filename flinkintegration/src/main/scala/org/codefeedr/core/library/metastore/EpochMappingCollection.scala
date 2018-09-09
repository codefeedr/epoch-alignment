package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkClient, ZkCollectionNode, ZkNodeBase}

class EpochMappingCollection(parent: ZkNodeBase)
                            (implicit override val zkClient: ZkClient)
    extends ZkCollectionNode[EpochMappingNode, Unit]("mappings",
                                                     parent,
                                                     (n, p) => new EpochMappingNode(n, p)) {}
