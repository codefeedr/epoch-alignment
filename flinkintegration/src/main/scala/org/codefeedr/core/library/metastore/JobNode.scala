package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkClient, ZkNode, ZkNodeBase}

case class FlinkJob(Sources: Array[String], Sinks: Array[String])

class JobNode(name: String, p: ZkNodeBase)(implicit override val zkClient: ZkClient) extends ZkNode[FlinkJob](name, p) with Serializable {}
