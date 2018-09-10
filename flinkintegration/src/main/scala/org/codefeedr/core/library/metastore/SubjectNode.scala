package org.codefeedr.core.library.metastore

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.core.library.internal.zookeeper._
import org.codefeedr.exceptions._
import org.codefeedr.model.SubjectType
import org.codefeedr.model.zookeeper.EpochCollection

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag


trait SubjectNode extends ZkStateNode[SubjectType,Boolean]{

  /**
    * Override to create child nodes
    *
    * @return
    */
  def postCreate(): Future[Unit]

  def create(data: SubjectType): Future[SubjectType]

  /**
    * Obtains the node maintaining the collection of consumers of the subject type
    *
    * @return
    */
  def getSinks(): QuerySinkCollection

  /**
    * Obtains the node maintaining the collection of producers for the subjectype
    *
    * @return
    */
  def getSources(): QuerySourceCollection

  /**
    * Obtains the node that contains the epochs that belong to this subject
    *
    * @return
    */
  def getEpochs(): EpochCollectionNode

  private[codefeedr] def forceUnRegisterSubject(): Future[Unit]

  private[codefeedr] def unregister(): Future[Boolean]

  /**
    * Retrieve value if the subject has an active consumer
    *
    * @return
    */
  def hasActiveSources(): Future[Boolean]

  /**
    * Retrieve a value if the given type has any active producers
    *
    * @return
    */
  def hasActiveSinks(): Future[Boolean]

  /**
    * Method that asserts the given typeName exists
    * Throws an exception if this is not the case
    *
    * @return A future that resolves when the check has completed
    */
  def assertExists(): Future[Unit]

  /**
    * Updates the state of the subjectnode
    * Reads the sinks. If all sinks are closed the subject will also be closed
    *
    * @return
    */
  def updateState(): Future[Unit]

  /**
    * Returns a future if the subject is still open
    *
    * @return a future with boolean if the type was still open
    */
  def isOpen(): Future[Boolean]

  /**
    * Constructs a future that resolves whenever the subject is closed
    *
    * @return A future that resolves when the type is closed
    */
  def awaitClose(): Future[Unit]

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


trait SubjectNodeComponent extends ZkStateNodeComponent {
  this: ZkClientComponent
  with QuerySinkCollectionComponent
  with QuerySourceCollectionComponent
  with EpochCollectionNodeComponent
  =>


  /**
    * This class contains services to obtain more detailed information about kafka partitions,
    * consumers (kafkasources) consuming these sources and their offsets
    */
  class SubjectNodeImpl(subjectName: String, parent: ZkNodeBase)
    extends ZkNodeImpl[SubjectType](subjectName, parent)
      with ZkStateNodeImpl[SubjectType, Boolean]
      with LazyLogging with SubjectNode {

    /**
      * Override to create child nodes
      *
      * @return
      */
    override def postCreate(): Future[Unit] = async {
      await(getSinks().create())
      await(getSources().create())
      //Create epochCollection with -1 as default latest epoch id
      await(getEpochs().create(EpochCollection(-1)))
      await(super.postCreate())
    }

    override def create(data: SubjectType): Future[SubjectType] = async {
      val r = await(super.create(data))
      logger.debug(s"Created subject node with name $name")
      r
    }

    /**
      * Obtains the node maintaining the collection of consumers of the subject type
      *
      * @return
      */
    override def getSinks(): QuerySinkCollection = new QuerySinkCollectionImpl(this)

    /**
      * Obtains the node maintaining the collection of producers for the subjectype
      *
      * @return
      */
    override def getSources(): QuerySourceCollection = new QuerySourceCollectionImpl(this)

    /**
      * Obtains the node that contains the epochs that belong to this subject
      *
      * @return
      */
    override def getEpochs(): EpochCollectionNode = new EpochCollectionNodeImpl(this)

    /**
      * Retrieve value if the subject has an active consumer
      *
      * @return
      */
    override def hasActiveSources(): Future[Boolean] = getSources().getState()

    /**
      * Retrieve a value if the given type has any active producers
      *
      * @return
      */
    override def hasActiveSinks(): Future[Boolean] = getSinks().getState()

    /**
      * Method that asserts the given typeName exists
      * Throws an exception if this is not the case
      *
      * @return A future that resolves when the check has completed
      */
    override def assertExists(): Future[Unit] = async {
      if (!await(exists())) {
        val error = TypeNameNotFoundException(subjectName)
        logger.error("Typename not found", error)
        throw error
      }
    }

    /**
      * Removes a subject and all its children without perfoming any checks
      * Meant to clean up zookeeper in tests
      *
      * @return a future that resolves when the delete is done
      */
    private[codefeedr] def forceUnRegisterSubject(): Future[Unit] =
      deleteRecursive()

    /**
      * Un-register a subject from the library
      *
      * @throws ActiveSinkException   when the subject still has an active sink
      * @throws ActiveSourceException when the subject still has an active source
      * @return A future that returns when the subject has actually been removed from the library
      */
    private[codefeedr] def unregister(): Future[Boolean] = async {
      logger.debug(s"Deleting subject $subjectName")
      if (await(exists())) {
        if (await(getSources().getState())) throw ActiveSourceException(subjectName)
        if (await(getSinks.getState())) throw ActiveSinkException(subjectName)
        deleteRecursive()
        true
      } else {
        logger.debug(s"$subjectName never existed, returning false")
        false
      }
    }

    /**
      * Updates the state of the subjectnode
      * Reads the sinks. If all sinks are closed the subject will also be closed
      *
      * @return
      */
    override def updateState(): Future[Unit] = async {
      logger.debug(s"Updating subject state of $name")

      val shouldClose = await(for {
        isOpen <- getState().map(o => o.get)
        persistent <- getData().map(o => o.get.persistent)
        hasSinks <- getSinks().getState()
      } yield (persistent, hasSinks, isOpen))

      if (!shouldClose._1 && !shouldClose._2 && shouldClose._3) {
        logger.info(
          s"Closing subject $name because no more sinks are active and subject is not persistent.")
        await(setState(false))
      } else {
        logger.debug(
          s"Not closing subject $name. persistent: ${shouldClose._1},  hasSinks: ${shouldClose._2},  isOpen: ${shouldClose._3}")
      }

      logger.debug(s"Updated subject state of $name")

    }

    /**
      * Returns a future if the subject is still open
      *
      * @return a future with boolean if the type was still open
      */
    override def isOpen(): Future[Boolean] =
      getState().map(o => o.get)

    /**
      * Constructs a future that resolves whenever the subject is closed
      *
      * @return A future that resolves when the type is closed
      */
    override def awaitClose(): Future[Unit] = {
      logger.info(s"Waiting for $name to close.")
      getStateNode().awaitCondition(o => !o).map(_ => ())
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
    override def initialState(): Boolean = true
  }

}