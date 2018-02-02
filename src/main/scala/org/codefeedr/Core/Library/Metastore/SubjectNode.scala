package org.codefeedr.Core.Library.Metastore

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.Core.Library.Internal.SubjectTypeFactory
import org.codefeedr.Core.Library.Internal.Zookeeper.{ZkClient, ZkNodeBase, ZkNode}
import org.codefeedr.Exceptions._
import org.codefeedr.Model.SubjectType

import scala.reflect.runtime.{universe => ru}
import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * This class contains services to obtain more detailed information about kafka partitions,
  * consumers (kafkasources) consuming these sources and their offsets
  */
class SubjectNode(subjectName: String, parent: ZkNodeBase)
    extends ZkNode[SubjectType](subjectName, parent)
    with LazyLogging {

  /**
    * Override to create child nodes
    * @return
    */
  override def PostCreate(): Future[Unit] = async {
    await(GetState().Create(true))
    await(GetSinks().Create())
    await(GetSources().Create())
  }

  /**
    * Retrieves child node representing the state of the subject (active or inactive)
    * @return
    */
  def GetState(): ZkNode[Boolean] = GetChild[Boolean]("state")

  /**
    * Obtains the node maintaining the collection of consumers of the subject type
    * @return
    */
  def GetSinks(): QuerySinkCollection = new QuerySinkCollection(this)

  /**
    * Obtains the node maintaining the collection of producers for the subjectype
    * @return
    */
  def GetSources(): QuerySourceCollection = new QuerySourceCollection(this)

  /**
    * Retrieve a subjectType for an arbitrary scala type
    * Creates type information and registers the type in the library
    * Creates a non-persistent type
    *
    * @tparam T The type to register
    * @return The subjectType when it is registered in the library
    */
  def GetOrCreateType[T: ru.TypeTag](): Future[SubjectType] =
    GetOrCreateType[T](persistent = false)

  /**
    * Retrieve a subjectType for an arbitrary scala type
    * Creates type information and registers the type in the library
    *
    * @param persistent Should the type, if it does not exist, be created as persistent?
    * @tparam T The type to register
    * @return The subjectType when it is registered in the library
    */
  def GetOrCreateType[T: ru.TypeTag](persistent: Boolean): Future[SubjectType] = {
    val name = SubjectTypeFactory.getSubjectName[T]
    logger.debug(s"Getting or creating type $name with persistency: $persistent")
    val factory = () => SubjectTypeFactory.getSubjectType[T](persistent)
    GetOrCreate(factory)
  }

  /**
    * Checks if the type is not persistent and there are no sinks.
    * If so it closes the subject
    *
    * @return A future that resolves when the operation is done
    */
  private def CloseIfNoSinks(typeName: String): Future[Unit] = async {
    //For non-persistent types automatically close the subject when all sources are removed
    val shouldClose = await(for {
      persistent <- GetData().map(o => o.get.persistent)
      hasSinks <- HasActiveSinks()
      isOpen <- IsOpen()
    } yield !persistent && !hasSinks && isOpen)

    if (shouldClose) {
      await(Close())
    }
  }

  /**
    * Retrieve value if the subject has an active consumer
    * @return
    */
  def HasActiveSources(): Future[Boolean] = GetSources().GetState()

  /**
    * Retrieve a value if the given type has any active producers
    * @return
    */
  def HasActiveSinks(): Future[Boolean] = GetSources().GetState()

  /**
    * Method that asserts the given typeName exists
    * Throws an exception if this is not the case
    *
    * @return A future that resolves when the check has completed
    */
  def AssertExists(): Future[Unit] = async {
    if (!await(Exists())) {
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
  private[codefeedr] def ForceUnRegisterSubject(): Future[Unit] =
    DeleteRecursive()

  /**
    * Un-register a subject from the library
    * @throws ActiveSinkException   when the subject still has an active sink
    * @throws ActiveSourceException when the subject still has an active source
    * @return A future that returns when the subject has actually been removed from the library
    */
  private[codefeedr] def Unregister(): Future[Boolean] = async {
    logger.debug(s"Deleting subject $subjectName")
    if (await(Exists())) {
      if (await(GetSources().GetState())) throw ActiveSourceException(subjectName)
      if (await(GetSinks.GetState())) throw ActiveSinkException(subjectName)
      DeleteRecursive()
      true
    } else {
      logger.debug(s"$subjectName never existed, returning false")
      false
    }
  }

  /**
    * Closes the subject
    *
    * @return a future that resolves when the write was succesful
    */
  def Close(): Future[Unit] = {
    logger.debug(s"closing subject $name")
    GetState().SetData(false)
  }

  /**
    * Returns a future if the subject is still open
    *
    * @return a future with boolean if the type was still open
    */
  def IsOpen(): Future[Boolean] =
    GetState().GetData().map(o => o.get)

  /**
    * Constructs a future that resolves whenever the subject is closed
    *
    * @return A future that resolves when the type is closed
    */
  def AwaitClose(): Future[Unit] =
    GetState().AwaitCondition(o => !o).map(_ => Unit)
}
