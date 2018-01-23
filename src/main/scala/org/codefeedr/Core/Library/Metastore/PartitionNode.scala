package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.{ZkClient, ZkNode, ZkNodeBase}
import org.codefeedr.Model.Zookeeper.Partition

class PartitionNode(val zk: ZkClient)(partitionNr:Int, parent: ZkNodeBase)
  extends ZkNode[Partition](s"$partitionNr", parent){

}
