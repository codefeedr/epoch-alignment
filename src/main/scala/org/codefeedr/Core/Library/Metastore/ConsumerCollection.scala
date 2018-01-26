package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.{ZkCollectionNode, ZkCollectionState, ZkNodeBase}
import org.codefeedr.Model.Zookeeper.Consumer

class ConsumerCollection(subjectName: String, parent: ZkNodeBase)
  extends ZkCollectionNode[ConsumerNode]("consumers", parent, (name, parent) => new ConsumerNode(name, parent))
    with ZkCollectionState[ConsumerNode, Consumer]{
}


