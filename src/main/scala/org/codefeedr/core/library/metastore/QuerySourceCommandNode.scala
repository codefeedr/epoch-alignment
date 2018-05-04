package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkCollectionNode, ZkNode, ZkNodeBase, ZkQueueNode}
import org.codefeedr.core.library.metastore.sourcecommand.SourceCommand
import scala.reflect._

import scala.reflect.ClassTag

class QuerySourceCommandNode(p: ZkNodeBase) extends ZkNode[Unit]("commands", p) with ZkQueueNode[Unit,SourceCommand] {
  override implicit def tag: ClassTag[SourceCommand] = classTag[SourceCommand]
}
