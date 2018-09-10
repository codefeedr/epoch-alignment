package org.codefeedr.core.library.metastore

import org.codefeedr.core.library.internal.zookeeper._
import org.codefeedr.model.zookeeper.Partition

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.ClassTag

trait EpochNode extends ZkStateNode[Epoch, Boolean] {

  /**
    * Retrieves the partitions that belong to this epoch
    *
    * @return the partitions of the epoch
    */
  def getPartitions(): EpochPartitionCollection

  def getMappings(): EpochMappingCollection

  /**
    * Retrieves all partition data of the epoch
    *
    * @return
    */
  def getPartitionData(): Future[Iterable[Partition]]

  def getEpoch(): Int

  def postCreate(): Future[Unit]

  /**
    * The base class needs to expose the typeTag, no typeTag constraints can be put on traits
    *
    * @return
    */
  def typeT(): ClassTag[Boolean]

  /**
    * The initial state of the node. State is not allowed to be empty
    *
    * @return
    */
  def initialState(): Boolean
}

trait EpochNodeComponent extends ZkStateNodeComponent {
  this: ZkClientComponent
    with EpochMappingCollectionComponent
    with EpochPartitionCollectionComponent =>

  /**
    * Node representing a single epoch under a source
    *
    * @param epoch
    * @param parent
    */
  class EpochNodeImpl(epoch: Int, parent: ZkNodeBase)
      extends ZkNodeImpl[Epoch](s"$epoch", parent)
      with ZkStateNodeImpl[Epoch, Boolean]
      with EpochNode {

    /**
      * Retrieves the partitions that belong to this epoch
      *
      * @return the partitions of the epoch
      */
    override def getPartitions(): EpochPartitionCollection = new EpochPartitionCollectionImpl(this)

    override def getMappings(): EpochMappingCollection = new EpochMappingCollectionImpl(this)

    /**
      * Retrieves all partition data of the epoch
      *
      * @return
      */
    override def getPartitionData(): Future[Iterable[Partition]] = {
      getPartitions()
        .getChildren()
        .flatMap(
          o =>
            Future.sequence(
              o.map(o => o.getData().map(o => o.get))
          )
        )
    }

    override def getEpoch(): Int = epoch

    override def postCreate(): Future[Unit] =
      for {
        _ <- getMappings().create()
        _ <- getPartitions().create()
        _ <- super.postCreate()
      } yield {}

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

}
