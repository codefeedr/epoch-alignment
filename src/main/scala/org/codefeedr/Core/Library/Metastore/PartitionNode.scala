package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.{ZkClient, ZkNode}
import org.codefeedr.Model.Zookeeper.Partition

class PartitionNode(val zk: ZkClient)(val subjectName: String, partitionNr:Int)
  extends ZkNode[Partition](zk)(s"/Codefeedr/Subjects/$subjectName/partitions/$partitionNr"){

}
