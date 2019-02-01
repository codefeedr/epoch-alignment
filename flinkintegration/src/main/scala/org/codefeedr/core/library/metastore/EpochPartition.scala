package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper._
import org.codefeedr.model.zookeeper.Partition

import scala.reflect.ClassTag

trait EpochPartition extends ZkStateNode[Partition, Boolean]

trait EpochPartitionComponent extends ZkStateNodeComponent { this: ZkClientComponent =>

  class EpochPartitionImpl(name: String, parent: ZkNodeBase)
      extends ZkNodeImpl[Partition](name, parent)
      with ZkStateNodeImpl[Partition, Boolean]
      with EpochPartition {
    override def typeT(): ClassTag[Boolean] = ClassTag(classOf[Boolean])

    override def initialState(): Boolean = false
  }

}
