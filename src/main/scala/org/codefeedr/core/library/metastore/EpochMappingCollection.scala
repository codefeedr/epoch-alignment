package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkCollectionNode, ZkNodeBase}

class EpochMappingCollection(parent: ZkNodeBase)
  extends ZkCollectionNode[EpochMappingNode, Unit]("mappings",
    parent,
    (n, p) => new EpochMappingNode(n, p)) {

}
