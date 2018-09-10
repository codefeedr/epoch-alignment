package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper._


trait EpochMappingCollection extends ZkCollectionNode[EpochMappingNode, Unit]

trait EpochMappingCollectionComponent extends ZkCollectionNodeComponent {
    this: ZkClientComponent
    with EpochMappingNodeComponent
  =>


  class EpochMappingCollectionImpl(parent: ZkNodeBase)
    extends ZkCollectionNodeImpl[EpochMappingNode, Unit]("mappings",
      parent,
      (n, p) => new EpochMappingNodeImpl(n, p))
      with EpochMappingCollection
  {}

}