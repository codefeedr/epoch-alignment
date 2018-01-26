package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.{ZkCollectionNode, ZkCollectionState, ZkNodeBase}
import org.codefeedr.Model.Zookeeper.QuerySource

import scala.concurrent.Future


class QuerySourceCollection(parent: ZkNodeBase)
  extends ZkCollectionNode[QuerySourceNode]("sources", parent, (name, parent) => new QuerySourceNode(name, parent))
    with ZkCollectionState[QuerySourceNode, QuerySource] {

}


