package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper._

trait EpochMappingNode extends ZkNode[EpochMapping]

trait EpochMappingNodeComponent extends ZkCollectionNodeComponent { this: ZkClientComponent =>

  class EpochMappingNodeImpl(subject: String, parent: ZkNodeBase)
      extends ZkNodeImpl[EpochMapping](subject, parent)
      with EpochMappingNode {}
}
