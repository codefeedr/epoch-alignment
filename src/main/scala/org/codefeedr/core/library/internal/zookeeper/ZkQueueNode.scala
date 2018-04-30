package org.codefeedr.core.library.internal.zookeeper

import scala.reflect.ClassTag

/**
  * Implementation of a queue
  *
  * @tparam TData Type of the data of the node
  */
class ZkQueueNode[TData: ClassTag, TElement: ClassTag](name: String, parent: ZkNodeBase)
    extends ZkNode[TData](name, parent) {}
