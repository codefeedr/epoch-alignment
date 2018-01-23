package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.{ZkClient, ZkCollectionNode, ZkNodeBase}


class ProducerCollection(subjectName: String, parent: ZkNodeBase)
  extends ZkCollectionNode[ProducerNode]("producers", parent, (name, parent) => new ProducerNode(name, parent)) {
}


