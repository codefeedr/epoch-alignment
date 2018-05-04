package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkNode, ZkNodeBase}

class EpochMappingNode(subject: String, parent : ZkNodeBase) extends ZkNode[EpochMapping](subject,parent){


}
