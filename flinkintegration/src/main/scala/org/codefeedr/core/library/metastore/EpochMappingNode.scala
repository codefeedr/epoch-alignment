package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkClient, ZkNode, ZkNodeBase}

class EpochMappingNode(subject: String, parent: ZkNodeBase)
                      (implicit override val zkClient: ZkClient)
    extends ZkNode[EpochMapping](subject, parent) {}
