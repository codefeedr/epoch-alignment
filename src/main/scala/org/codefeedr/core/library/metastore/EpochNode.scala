package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper.{ZkNode, ZkNodeBase, ZkStateNode}
import org.codefeedr.model.zookeeper.Partition

import scala.async.Async.{await, async}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.ClassTag

/**
  * Node representing a single epoch under a source
  * @param epoch
  * @param parent
  */
class EpochNode(epoch: Int, parent: ZkNodeBase)
    extends ZkNode[Epoch](s"$epoch", parent)
    with ZkStateNode[Epoch, Boolean] {

  /**
    * Retrieves the partitions that belong to this epoch
    * @return the partitions of the epoch
    */
  def getPartitions(): EpochPartitionCollection = new EpochPartitionCollection(this)

  def getMappings(): EpochMappingCollection = new EpochMappingCollection(this)

  /**
    * Retrieves all partition data of the epoch
    * @return
    */
  def getPartitionData(): Future[Iterable[Partition]] = {
    getPartitions()
      .getChildren()
      .flatMap(
        o =>
          Future.sequence(
            o.map(o => o.getData().map(o => o.get))
        )
      )
  }

  def getEpoch(): Int = epoch

  override def postCreate(): Future[Unit] = async {
    await(getMappings().create())
    await(getPartitions().create())
  }

  /**
    * The base class needs to expose the typeTag, no typeTag constraints can be put on traits
    *
    * @return
    */
  override def typeT(): ClassTag[Boolean] = ClassTag(classOf[Boolean])

  /**
    * The initial state of the node. State is not allowed to be empty
    *
    * @return
    */
  override def initialState(): Boolean = false
}
