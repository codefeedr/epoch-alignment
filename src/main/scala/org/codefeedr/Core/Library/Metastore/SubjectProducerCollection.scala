package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.{ZkClient, ZkCollectionNode, ZkNodeBase}
import org.codefeedr.Model.Zookeeper.{Partition, Producer}

class ProducerCollection(subjectName: String, parent: ZkNodeBase)
  extends ZkCollectionNode[Producer]("partitions", parent) {

  override GetChild(name: String):Producer =
}


