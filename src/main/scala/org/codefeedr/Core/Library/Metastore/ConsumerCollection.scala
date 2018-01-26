package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.{ZkCollectionNode, ZkCollectionState, ZkNodeBase}

class ConsumerCollection(subjectName: String, parent: ZkNodeBase)
  extends ZkCollectionNode[ConsumerNode]("consumers", parent, (name, parent) => new ConsumerNode(name, parent))
    with ZkCollectionState[ConsumerNode]{
}


