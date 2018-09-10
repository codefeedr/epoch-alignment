package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{
  ZkClient,
  ZkClientComponent,
  ZkNodeBase,
  ZkNodeBaseComponent
}

trait MetaRootNode extends ZkNodeBase {

  def parent(): ZkNodeBase

  def path(): String

  def getSubjects(): SubjectCollectionNode

  def getJobs(): JobNodeCollection
}

trait MetaRootNodeComponent extends ZkNodeBaseComponent {
  this: ZkClientComponent with JobNodeCollectionComponent with SubjectCollectionNodeComponent =>

  /**
    * Root node used for zookeeper configuration
    */
  class MetaRootNodeImpl extends ZkNodeBaseImpl("Metastore") with MetaRootNode {
    override def parent(): ZkNodeBase = null

    override def path(): String = s"/$name"

    override def getSubjects() = new SubjectCollectionNodeImpl(this)

    override def getJobs() = new JobNodeCollectionImpl(this)
  }

}
