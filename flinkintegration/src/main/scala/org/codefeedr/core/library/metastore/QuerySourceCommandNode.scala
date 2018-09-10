package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper._
import org.codefeedr.core.library.metastore.sourcecommand.SourceCommand

import scala.reflect._
import scala.reflect.ClassTag

trait QuerySourceCommandNode extends ZkQueueNode[Unit, SourceCommand]

trait QuerySourceCommandNodeComponent extends ZkQueueNodeComponent { this: ZkClientComponent =>

  class QuerySourceCommandNodeImpl(p: ZkNodeBase)
      extends ZkNodeImpl[Unit]("commands", p)
      with ZkQueueNodeImpl[Unit, SourceCommand]
      with QuerySourceCommandNode {

    override implicit def tag: ClassTag[SourceCommand] = classTag[SourceCommand]
  }

}
