package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.{ZkCollectionNode, ZkCollectionState, ZkNodeBase}
import org.codefeedr.Model.Zookeeper.QuerySink

import scala.concurrent.Future


class QuerySinkCollection(parent: ZkNodeBase)
  extends ZkCollectionNode[QuerySinkNode]("sinks", parent, (name, parent) => new QuerySinkNode(name, parent))
  with ZkCollectionState[QuerySinkNode, QuerySink] {



}


