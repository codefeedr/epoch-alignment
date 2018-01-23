package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.{ZkCollectionNode, ZkNodeBase}

class ConsumerCollection(subjectName: String, parent: ZkNodeBase)
  extends ZkCollectionNode[ConsumerNode]("partitions", parent, (name, parent) => new ConsumerNode(name, parent)) {
}


