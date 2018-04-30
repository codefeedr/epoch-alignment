package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkNode, ZkNodeBase}

class QuerySourceCommandNode(p: ZkNodeBase) extends ZkNode[Unit]("commands", p) {}
