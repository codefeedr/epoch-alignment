package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.ZkNodeBase

/**
  * Root node used for zookeeper configuration
  */
class MetaRootNode extends ZkNodeBase("Metastore") {
  override def Parent(): ZkNodeBase = null
  override def Path(): String = s"/$name"

  def GetSubjects() = new SubjectCollectionNode(this)
}
