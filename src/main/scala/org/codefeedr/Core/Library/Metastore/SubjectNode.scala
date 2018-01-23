package org.codefeedr.Core.Library.Metastore

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.Core.Library.Internal.SubjectTypeFactory
import org.codefeedr.Core.Library.Internal.Zookeeper.{ZkClient, ZkNodeBase, ZkNode}
import org.codefeedr.Exceptions._
import org.codefeedr.Model.SubjectType

import scala.reflect.runtime.{universe => ru}
import scala.async.Async.{async, await}
import scala.concurrent.Future


/**
  * This class contains services to obtain more detailed information about kafka partitions,
  * consumers (kafkasources) consuming these sources and their offsets
  */
class SubjectNode(subjectName: String, parent: ZkNodeBase)
  extends ZkNode[SubjectType](subjectName, parent)
    with LazyLogging {

  /**
    * Retrieves child node representing the state of the subject (active or inactive)
    * @return
    */
  def GetState(): ZkNode[Boolean] = GetChild[Boolean]("state")
  def GetSource(): ZkNodeBase = GetChild("source")
  def GetSink(): ZkNodeBase = GetChild("sink")



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
    GetOrCreate(factory)(() => async {
      await(GetState().Create(true))
      await(GetSource().Create())
      await(GetSink().Create())
    })
  }


  /**
    * Register the given uuid as sink of the given type
    *
    * @param typeName name of the type to register the sink for
    * @param uuid     uuid of the sink
    * @throws TypeNameNotFoundException      when typeName is not registered
    * @throws SinkAlreadySubscribedException when the given uuid was already registered as sink on the given type
    * @return A future that resolves when the registration is complete
    */
  def RegisterSink(typeName: String, uuid: String): Future[Unit] = async {
    //Check if subject exists
    await(AssertExists())

    val sinkNode = GetSinkNode(typeName, uuid)

    //Check if sink does not exist on type
    if (await(sinkNode.Exists)) {
      throw SinkAlreadySubscribedException(uuid.concat(" on ").concat(typeName))
    }

    logger.debug(s"Registering sink $uuid on $typeName")

    await(sinkNode.Create())
  }

  /**
    * Register the given uuid as source of the given type
    *
    * @param typeName name of the type to register the source for
    * @param uuid     uuid of the source
    * @throws TypeNameNotFoundException      when typeName is not registered
    * @throws SinkAlreadySubscribedException when the given uuid was already registered as source on the given type
    * @return A future that resolves when the registration is complete
    */
  def RegisterSource(typeName: String, uuid: String): Future[Unit] = async {
    //Check if type exists
    await(AssertExists(typeName))
    val sourceNode = GetSourceNode(typeName, uuid)

    //Check if sink does not exist on type
    if (await(sourceNode.Exists)) {
      throw SourceAlreadySubscribedException(uuid.concat(" on ").concat(typeName))
    }

    await(sourceNode.Create())
  }

  /**
    * Unregisters the given sink uuid as sink of the given type
    *
    * @param typeName name of the type to remove the sink from
    * @param uuid     uuid of the sink
    * @throws TypeNameNotFoundException  when typeName is not registered
    * @throws SinkNotSubscribedException when the given uuid was not registered as sink on the given type
    * @return A future that resolves when the removal is complete
    */
  def UnRegisterSink(typeName: String, uuid: String): Future[Unit] = async {
    //Check if type exists
    await(AssertExists(typeName))
    val sinkNode = GetSinkNode(typeName, uuid)

    //Check if sink does not exist on type
    if (!await(sinkNode.Exists)) {
      throw SinkNotSubscribedException(uuid.concat(" on ").concat(typeName))
    }
    await(sinkNode.Delete)

    logger.debug(s"${await(GetSinks(typeName)).size} sinks remaining. Persistent: ${await(
      GetType(typeName).map(o => o.get.persistent))}")

    //Check if the type needs to be removed
    await(CloseIfNoSinks(typeName))

    await(DeleteIfNoSourcesAndSinks(typeName))
  }

  /**
    * Unregisters the given source uuid as source of the given type
    *
    * @param typeName name of the type to remove the source from
    * @param uuid     uuid of the source
    * @throws TypeNameNotFoundException  when typeName is not registered
    * @throws SinkNotSubscribedException when the given uuid was not registered as source on the given type
    * @return A future that resolves when the removal is complete
    */
  def UnRegisterSource(typeName: String, uuid: String): Future[Unit] = async {
    //Check if type exists
    await(AssertExists(typeName))
    val sourceNode = GetSourceNode(typeName, uuid)

    //Check if sink does not exist on type
    if (!await(sourceNode.Exists)) {
      throw SourceNotSubscribedException(uuid.concat(" on ").concat(typeName))
    }
    await(sourceNode.Delete)

    //Close and/or remove the subject
    await(DeleteIfNoSourcesAndSinks(typeName))
  }

  /**
    * Checks if the type is not persistent and there are no sinks.
    * If so it closes the subject
    *
    * @param typeName Type to check
    * @return A future that resolves when the operation is done
    */
  private def CloseIfNoSinks(typeName: String): Future[Unit] = async {
    //For non-persistent types automatically close the subject when all sources are removed
    val shouldClose = await(for {
      persistent <- GetType(typeName).map(o => o.get.persistent)
      hasSinks <- HasSinks(typeName)
      isOpen <- IsOpen(typeName)
    } yield !persistent && !hasSinks && isOpen)

    if (shouldClose) {
      await(Close(typeName))
    }
  }



  /**
    * Checks if the given type is persistent
    * If not, and the type has no sinks or sources, removes the entire type
    * TODO: Temporary disabled, because we need to discuss if we even want this
    * @return A future that resolves when the operation is done
    */
  private def DeleteIfNoSourcesAndSinks(typeName: String): Future[Unit] = async {

    /*
    val shouldRemove = await(for {
      persistent <- GetType(typeName).map(o => o.get.persistent)
      hasSources <- HasSources(typeName)
      hasSinks <- HasSinks(typeName)
    } yield !persistent && !hasSources && !hasSinks)

    if (shouldRemove) {
      await(UnRegisterSubject(typeName))
    }
   */
  }

  /**
    * Gets a list of uuids for all sinks that are subscribed on the type
    *
    * @param typeName type to check for sinks
    * @throws TypeNameNotFoundException when typeName is not registered
    * @return A future that resolves with the registered sinks
    */
  def GetSinks(typeName: String): Future[Array[String]] = async {
    await(AssertExists(typeName))
    val sinkNode = GetSinkNode(typeName)
    await(sinkNode.GetChildren()).map(o => o.Name).toArray
  }

  /**
    * Gets a list of uuids for all sources that are subscribed on the type
    *
    * @param typeName type to check for sources
    * @throws TypeNameNotFoundException when typeName is not registered
    * @return A future that resolves with the registered sources
    */
  def GetSources(typeName: String): Future[Array[String]] = async {
    await(AssertExists(typeName))
    val sourceNode = GetSourceNode(typeName)
    await(sourceNode.GetChildren()).map(o => o.Name).toArray
  }

  /**
    * Retrieve a value if the given type has active sinks
    *
    * @param typeName name of the type to check for active sinks
    * @throws TypeNameNotFoundException when typeName is not registered
    * @return Future that will resolve into boolean if a sink exists
    */
  def HasSinks(): Future[Boolean] = GetSinks(typeName).map(o => o.nonEmpty)

  /**
    * Retrieve a value if the given type has active sources
    *
    * @param typeName name of the type to check for active sinks
    * @throws TypeNameNotFoundException when typeName is not registered
    * @return Future that will resolve into boolean if a source exists
    */
  def HasSources(): Future[Boolean] = GetSources(typeName).map(o => o.nonEmpty)

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
    * @param name the subject to delete
    * @return a future that resolves when the delete is done
    */
  private[codefeedr] def ForceUnRegisterSubject(name: String): Future[Unit] =
    DeleteRecursive()

  /**
    * Un-register a subject from the library
    * @throws ActiveSinkException   when the subject still has an active sink
    * @throws ActiveSourceException when the subject still has an active source
    * @return A future that returns when the subject has actually been removed from the library
    */
  private[codefeedr] def Unregister(): Future[Boolean] = async {
    logger.debug(s"Deleting subject $subjectName")
    if(await(Exists())) {
      if (await(HasSinks())) throw ActiveSinkException(subjectName)
      if (await(HasSources())) throw ActiveSourceException(subjectName)
      DeleteRecursive()
      true
    } else {
      logger.debug(s"$subjectName never existed, returning false")
      false
    }
  }

  /**
    * Closes the subjectType
    *
    * @param name name of the subject to close
    * @return a future that resolves when the write was succesful
    */
  def Close(name: String): Future[Unit] = {
    logger.debug(s"closing subject $name")
    GetState().SetData(false)
  }

  /**
    * Returns a future if the subject with the given name is still open
    *
    * @param name name of the type
    * @return a future with boolean if the type was still open
    */
  def IsOpen(name: String): Future[Boolean] =
    GetState().GetData().map(o => o.get)

  /**
    * Constructs a future that resolves whenever the given type closes
    *
    * @param name name of the type to wait for
    * @return A future that resolves when the type is closed
    */
  def AwaitClose(name: String): Future[Unit] =
    GetState().AwaitCondition(o => !o).map(_ => Unit)
}
