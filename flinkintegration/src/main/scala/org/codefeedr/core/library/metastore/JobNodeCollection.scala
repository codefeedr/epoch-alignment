package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper._

trait JobNodeCollection extends ZkCollectionNode[JobNode, Unit]

trait JobNodeCollectionComponent extends ZkCollectionNodeComponent {
  this: ZkClientComponent with JobNodeComponent =>

  class JobNodeCollectionImpl(parent: ZkNodeBase)
      extends ZkCollectionNodeImpl[JobNode, Unit]("jobs", parent, (n, p) => new JobNodeImpl(n, p))
      with JobNodeCollection {}

}
