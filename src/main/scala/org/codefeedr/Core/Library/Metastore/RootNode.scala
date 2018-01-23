package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.ZkNodeBase

/**
  * Root node used for zookeeper configuration
  */
class RootNode extends ZkNodeBase("Codefeedr") {
  override def Parent(): ZkNodeBase = null
  override def Path(): String = s"/$name"

  def GetSubjects() = new SubjectCollectionNode(this)
}
