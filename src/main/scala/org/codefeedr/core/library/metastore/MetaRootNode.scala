package org.codefeedr.core.Library.Metastore

import org.codefeedr.core.Library.Internal.Zookeeper.ZkNodeBase

/**
  * Root node used for zookeeper configuration
  */
class MetaRootNode extends ZkNodeBase("Metastore") {
  override def Parent(): ZkNodeBase = null
  override def Path(): String = s"/$name"

  def GetSubjects() = new SubjectCollectionNode(this)
}
