package org.codefeedr.Core.Library.Metastore

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.Core.Library.Internal.Zookeeper.ZkClient


/**
  * This class contains services to obtain more detailed information about kafka partitions,
  * consumers (kafkasources) consuming these sources and their offsets
  */
class PartitionLibrary(val zk: ZkClient) extends LazyLogging {
  @transient private val SubjectPath = "/Codefeedr/Subjects"

  @transient implicit val zkClient: ZkClient = zk


}
