package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkCollectionNode, ZkNodeBase}

class JobNodeCollection(parent: ZkNodeBase)
    extends ZkCollectionNode[JobNode, Unit]("jobs", parent, (n, p) => new JobNode(n, p)) {}
