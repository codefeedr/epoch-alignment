package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkClient, ZkCollectionNode, ZkNodeBase}

class JobNodeCollection(parent: ZkNodeBase)(implicit override val zkClient: ZkClient)
    extends ZkCollectionNode[JobNode, Unit]("jobs", parent, (n, p) => new JobNode(n, p)) {}
