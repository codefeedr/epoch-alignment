package org.codefeedr.Core.Library.Metastore

import org.codefeedr.Core.Library.Internal.Zookeeper.{ZkClient, ZkCollectionNode}
import org.codefeedr.Model.SubjectType

class SubjectCollectionNode(val zk: ZkClient) extends ZkCollectionNode[SubjectType](zk)("/Codefeedr/Subjects")
{
  override def ChildConstructor(name: String) = SubjectNode(zkClient)(name)
}
