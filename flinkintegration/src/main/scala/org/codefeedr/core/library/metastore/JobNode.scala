package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper._

case class FlinkJob(Sources: Array[String], Sinks: Array[String])

trait JobNode extends ZkNode[FlinkJob]

trait JobNodeComponent extends ZkNodeComponent {
  this:ZkClientComponent =>

  class JobNodeImpl(name: String, p: ZkNodeBase)
    extends ZkNodeImpl[FlinkJob](name, p)
    with Serializable
    with JobNode{}

}
