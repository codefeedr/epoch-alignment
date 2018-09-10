package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.kafka.meta.SourceEpoch
import org.codefeedr.core.library.internal.zookeeper._

trait SourceEpochNode extends ZkNode[SourceEpoch]

trait SourceEpochNodeComponent extends ZkNodeComponent { this: ZkClientComponent =>

  /**
    * Node describing the epoch of a source of a job
    *
    * @param epoch  The epoch the node describes
    * @param parent Parent of the node
    */
  class SourceEpochNodeImpl(epoch: Int, parent: ZkNodeBase)
      extends ZkNodeImpl[SourceEpoch](s"$epoch", parent)
      with SourceEpochNode {}

}
