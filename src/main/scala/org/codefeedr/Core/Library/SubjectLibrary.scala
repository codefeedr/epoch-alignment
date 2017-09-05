/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.codefeedr.Core.Library

import com.typesafe.scalalogging.LazyLogging
import org.apache.zookeeper.KeeperException.NodeExistsException
import org.codefeedr.Core.Library.Internal.Serialisation.GenericSerialiser
import org.codefeedr.Core.Library.Internal.SubjectTypeFactory
import org.codefeedr.Core.Library.Internal.Zookeeper.ZkNode
import org.codefeedr.Exceptions._
import org.codefeedr.Model.SubjectType

import scala.async.Async.{async, await}
import scala.collection.immutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.runtime.{universe => ru}

/**
  * ThreadSafe
  * Created by Niels on 14/07/2017.
  */
object SubjectLibrary extends LazyLogging {
  //Zookeeper path where the subjects are stored
  @transient private val SubjectPath = "/Codefeedr/Subjects"

  /**
    * Get the path to the zookeeper definition of the given subject
    * @param s the name of the subject
    * @return the full path to the subject
    */
  private def GetSubjectNode(s: String): ZkNode = ZkNode(s"$SubjectPath/$s")
  private def GetStateNode(s: String): ZkNode = ZkNode(s"$SubjectPath/$s/state")
  private def GetSourceNode(s: String): ZkNode = ZkNode(s"$SubjectPath/$s/source")
  private def GetSourceNode(s: String, uuid: String): ZkNode =
    ZkNode(s"$SubjectPath/$s/source/$uuid")
  private def GetSinkNode(s: String): ZkNode = ZkNode(s"$SubjectPath/$s/sink")
  private def GetSinkNode(s: String, uuid: String): ZkNode = ZkNode(s"$SubjectPath/$s/sink/$uuid")

  /**
    * Initalisation method
    * @return true when initialisation is done
    */
  def Initialize(): Future[Boolean] =
    ZkNode(SubjectPath).Create().map(_ => true)

  /**
    * Retrieve a subjectType for an arbitrary scala type
    * Creates type information and registers the type in the library
    * Creates a non-persistent type
    * @tparam T The type to register
    * @return The subjectType when it is registered in the library
    */
  def GetOrCreateType[T: ru.TypeTag](): Future[SubjectType] =
    GetOrCreateType[T](persistent = false)

  /**
    * Retrieve a subjectType for an arbitrary scala type
    * Creates type information and registers the type in the library
    * @param persistent Should the type, if it does not exist, be created as persistent?
    * @tparam T The type to register
    * @return The subjectType when it is registered in the library
    */
  def GetOrCreateType[T: ru.TypeTag](persistent: Boolean): Future[SubjectType] = {
    val name = SubjectTypeFactory.getSubjectName[T]
    val provider = () => SubjectTypeFactory.getSubjectType[T](persistent)
    GetOrCreateType(name, provider)
  }

  /**
    * Retrieves a subjecttype from the store if one is registered
    * Otherwise registeres the type in the store
    * @param subjectName Name of the subject to retrieve
    * @return
    */
  def GetOrCreateType(subjectName: String,
                      subjectProvider: () => SubjectType): Future[SubjectType] = async {

    if (await(Exists(subjectName))) {
      await(GetType(subjectName).map(o => o.get))
    } else {
      await(RegisterAndAwaitType(subjectProvider()))
    }

  }

  /**
    * Retrieve the subjectType for the given typename
    * @param typeName name of the type
    * @return Future with the subjecttype (or nothing if not found)
    */
  def GetType(typeName: String): Future[Option[SubjectType]] =
    GetSubjectNode(typeName).GetData[SubjectType]()

  /**
    * Retrieves the current set of registered subject names
    * @return A future with the set of registered subjects
    */
  def GetSubjectNames(): Future[immutable.Set[String]] = async {
    await(ZkNode(SubjectPath).GetChildren()).map(o => o.Name).toSet
  }

  /**
    * Registers the given subjectType, or if the subjecttype with the same name has already been registered, returns the already registered type with the same name
    * Returns a value once the requested type has been found
    * TODO: Acually check if the returned type is the same, and deal with duplicate type definitions
    * @tparam T Type to register
    * @return The subjectType once it has been registered
    */
  private def RegisterAndAwaitType[T: ru.TypeTag](): Future[SubjectType] = {
    val typeDef = SubjectTypeFactory.getSubjectType[T]
    RegisterAndAwaitType(typeDef).map(_ => typeDef)
  }

  /**
    * Register a type and resolve the future once the type has been registered
    * Returns a value once the requested type has been found
    * New types are automatically created in the open state
    * TODO: Using ZK ACL?
    * Returns true if the type could be registered
    * @param subjectType Type to register or retrieve
    * @return The subjectType once it has been registered
    */
  private def RegisterAndAwaitType(subjectType: SubjectType)(): Future[SubjectType] = {
    logger.debug(s"Registering new type ${subjectType.name}")
    async {
      await(GetSubjectNode(subjectType.name).Create[SubjectType](subjectType))
      await(GetStateNode(subjectType.name).Create[Boolean](true))
      await(GetSinkNode(subjectType.name).Create())
      await(GetSourceNode(subjectType.name).Create())
      subjectType
    }.recoverWith { //Register type. When error because node already exists just retrieve this value because the first writer wins.
      case _: NodeExistsException => GetType(subjectType.name).map(o => o.get)
    }
  }

  /**
    * Returns a future that contains the subjectType of the given name. Waits until the given type actually gets registered
    * @param typeName name of the type to find
    * @return future that will resolve when the given type has been found
    */
  def AwaitTypeRegistration(typeName: String): Future[SubjectType] = async {
    await(ZkNode(SubjectPath).AwaitChild(typeName))
    //Make sure to await the last registered child node, so the method does not return until the type has fully been registered
    //await(GetSubjectNode(typeName).AwaitChild("source"))
    await(GetType(typeName).map(o => o.get))
  }

  /**
    * Register the given uuid as sink of the given type
    * @param typeName name of the type to register the sink for
    * @param uuid uuid of the sink
    * @throws TypeNameNotFoundException when typeName is not registered
    * @throws SinkAlreadySubscribedException when the given uuid was already registered as sink on the given type
    * @return A future that resolves when the registration is complete
    */
  def RegisterSink(typeName: String, uuid: String): Future[Unit] = async {
    //Check if type exists
    await(AssertExists(typeName))
    val sinkNode = GetSinkNode(typeName, uuid)

    //Check if sink does not exist on type
    if (await(sinkNode.Exists)) {
      throw SinkAlreadySubscribedException(uuid.concat(" on ").concat(typeName))
    }

    await(sinkNode.Create())
  }

  /**
    * Register the given uuid as source of the given type
    * @param typeName name of the type to register the source for
    * @param uuid uuid of the source
    * @throws TypeNameNotFoundException when typeName is not registered
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
    * @param typeName name of the type to remove the sink from
    * @param uuid uuid of the sink
    * @throws TypeNameNotFoundException when typeName is not registered
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

    //Check if the type needs to be removed
    await(CloseIfNoSinks(typeName))
    await(DeleteIfNoSourcesAndSinks(typeName))
  }

  /**
    * Unregisters the given source uuid as source of the given type
    * @param typeName name of the type to remove the source from
    * @param uuid uuid of the source
    * @throws TypeNameNotFoundException when typeName is not registered
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
    * @return A future that resolves when the operation is done
    */
  private def DeleteIfNoSourcesAndSinks(typeName: String): Future[Unit] = async {
    val shouldRemove = await(for {
      persistent <- GetType(typeName).map(o => o.get.persistent)
      hasSources <- HasSources(typeName)
      hasSinks <- HasSinks(typeName)
    } yield !persistent && !hasSources && !hasSinks)

    if (shouldRemove) {
      await(UnRegisterSubject(typeName))
    }
  }

  /**
    * Gets a list of uuids for all sinks that are subscribed on the type
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
    * @param typeName name of the type to check for active sinks
    * @throws TypeNameNotFoundException when typeName is not registered
    * @return Future that will resolve into boolean if a sink exists
    */
  def HasSinks(typeName: String): Future[Boolean] = GetSinks(typeName).map(o => o.nonEmpty)

  /**
    * Retrieve a value if the given type has active sources
    * @param typeName name of the type to check for active sinks
    * @throws TypeNameNotFoundException when typeName is not registered
    * @return Future that will resolve into boolean if a source exists
    */
  def HasSources(typeName: String): Future[Boolean] = GetSources(typeName).map(o => o.nonEmpty)

  /**
    * Method that asserts the given typeName exists
    * Throws an exception if this is not the case
    * @param typeName The name of the type to check
    * @return A future that resolves when the check has completed
    */
  def AssertExists(typeName: String): Future[Unit] = async {
    if (!await(Exists(typeName))) {
      throw TypeNameNotFoundException(typeName)
    }
  }

  /**
    * Removes a subject and all its children without perfoming any checks
    * @param name the subject to delete
    * @return a future that resolves when the delete is done
    */
  private[codefeedr] def ForceUnRegisterSubject(name: String): Future[Unit] =
    GetSubjectNode(name).DeleteRecursive

  /**
    * Un-register a subject from the library
    * TODO: Refactor this to some recursive delete
    * @throws ActiveSinkException when the subject still has an active sink
    * @throws ActiveSourceException when the subject still has an active source
    * @param name: String
    * @return A future that returns when the subject has actually been removed from the library
    */
  private[codefeedr] def UnRegisterSubject(name: String): Future[Boolean] = {
    logger.debug(s"Deleting type $name")

    Exists(name).flatMap(o =>
      if (o) {
        async {
          if (await(HasSinks(name))) throw ActiveSinkException(name)
          if (await(HasSources(name))) throw ActiveSourceException(name)
          await(GetSubjectNode(name).DeleteRecursive)
          true
        }
      } else {
        //Return false because subject was not deleted
        Future.successful(false)
    })
  }

  /**
    * Gives true if the given type was defined in the storage
    * @tparam T Type to know if it was defined
    * @return
    */
  def Exists[T: ru.TypeTag]: Future[Boolean] = Exists(SubjectTypeFactory.getSubjectName[T])

  /**
    * Gives a future that is true wif the given type is defined
    * @param name name of the type that exists or not
    * @return
    */
  def Exists(name: String): Future[Boolean] = GetSubjectNode(name).Exists

  /**
    * Closes the subjectType
    * @param name name of the subject to close
    * @return a future that resolves when the write was succesful
    */
  def Close(name: String): Future[Unit] = {
    logger.debug(s"closing subject $name")
    GetStateNode(name).SetData[Boolean](false)
  }

  /**
    * Returns a future if the subject with the given name is still open
    * @param name name of the type
    * @return a future with boolean if the type was still open
    */
  def IsOpen(name: String): Future[Boolean] =
    GetStateNode(name).GetData[Boolean]().map(o => o.get)

  /**
    * Constructs a future that resolves whenever the given type closes
    * @param name name of the type to wait for
    * @return A future that resolves when the type is closed
    */
  def AwaitClose(name: String): Future[Unit] =
    GetStateNode(name).AwaitCondition[Boolean](o => !o).map(_ => Unit)

  /**
    * Constructs a future that resolves whenever the type is removed
    * @param name name of the type to watch
    * @return a future that resolves when the type has been removed
    */
  def AwaitRemove(name: String): Future[Unit] = GetSubjectNode(name).AwaitRemoval()

}
