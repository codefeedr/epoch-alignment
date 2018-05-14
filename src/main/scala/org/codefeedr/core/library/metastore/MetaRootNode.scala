package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.ZkNodeBase

/**
  * Root node used for zookeeper configuration
  */
class MetaRootNode extends ZkNodeBase("Metastore") {
  override def parent(): ZkNodeBase = null
  override def path(): String = s"/$name"

  def getSubjects() = new SubjectCollectionNode(this)
  def getJobs() = new JobNodeCollection(this)
}
