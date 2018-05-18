package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkNode, ZkNodeBase}

case class FlinkJob(Sources: Array[String], Sinks: Array[String])

class JobNode(name: String, p: ZkNodeBase) extends ZkNode[FlinkJob](name, p) with Serializable {}
