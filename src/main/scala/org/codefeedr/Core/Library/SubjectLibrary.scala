/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.codefeedr.Core.Library

import java.util.UUID

import akka.actor.ActorSystem
import com.twitter.zk.ZkClient
import com.typesafe.scalalogging.LazyLogging
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException.NodeExistsException
import org.apache.zookeeper.ZooDefs.Ids._
import org.codefeedr.Core.Library.Internal.Serialisation.{GenericDeserialiser, GenericSerialiser}
import org.codefeedr.Core.Library.Internal.SubjectTypeFactory
import org.codefeedr.Core.Library.Internal.Zookeeper.{ZkUtil, ZookeeperConfig}
import org.codefeedr.Exceptions._
import org.codefeedr.Model.SubjectType
import org.codefeedr.TwitterUtils._

import scala.async.Async.{async, await}
import scala.collection.immutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.reflect.runtime.{universe => ru}

/**
  * ThreadSafe
  * Created by Niels on 14/07/2017.
  */
object SubjectLibrary extends LazyLogging {
  //Topic used to publish all types and topics on
  //MAke this configurable?
  @transient private val SubjectTopic = "Subjects"

  //Zookeeper path where the subjects are stored
  @transient private val SubjectPath = "/Codefeedr/Subjects"

  @transient private val SubjectAwaitTime = 10
  @transient private val PollTimeout = 1000
  @transient private lazy val system = ActorSystem("SubjectLibrary")
  @transient private lazy val uuid = UUID.randomUUID()

  @transient private lazy val Deserialiser = new GenericDeserialiser[SubjectType]()
  @transient private lazy val Serialiser = new GenericSerialiser[SubjectType]()

  @transient private lazy val zk: ZkClient = ZookeeperConfig.getClient

  /**
    * Initialisation of zookeeper
    */
  @transient val Initialized: Future[Boolean] = async {
    if (!await(ZkUtil.pathExists("/Codefeedr"))) {
      await(ZkUtil.Create("/Codefeedr"))
    }
    if (!await(ZkUtil.pathExists(SubjectPath))) {
      await(ZkUtil.Create(SubjectPath))
    }
  }.map(_ => true)

  /**
    * Get the path to the zookeeper definition of the given subject
    * @param s the name of the subject
    * @return the full path to the subject
    */
  private def GetSubjectPath(s: String): String = SubjectPath.concat("/").concat(s)
  private def GetStatePath(s: String): String = SubjectPath.concat("/").concat(s).concat("/state")
  private def GetSourcePath(s: String): String =
    SubjectPath.concat("/").concat(s).concat("/source")
  private def GetSourcePath(s: String, uuid: String): String =
    SubjectPath.concat("/").concat(s).concat("/source").concat("/").concat(uuid)
  private def GetSinkPath(s: String): String = SubjectPath.concat("/").concat(s).concat("/sink")
  private def GetSinkPath(s: String, uuid: String): String =
    SubjectPath.concat("/").concat(s).concat("/sink").concat("/").concat(uuid)

  /**
    * Retrieve a subjectType for an arbitrary scala type
    * Creates type information and registers the type in the library
    * @tparam T The type to register
    * @return The subjectType when it is registered in the library
    */
  def GetOrCreateType[T: ru.TypeTag](): Future[SubjectType] = {
    val name = SubjectTypeFactory.getSubjectName[T]
    val provider = () => SubjectTypeFactory.getSubjectType[T]
    GetOrCreateType(name, provider)
  }

  /**
    * Retrieves a subjecttype from the store if one is registered
    * Otherwise registeres the type in the store
    * @param subjectName Name of the subject to retrieve
    * @return
    */
  def GetOrCreateType(subjectName: String,
                      subjectProvider: () => SubjectType): Future[SubjectType] = {
    async {
      if (await(Exists(subjectName))) {
        await(GetType(subjectName))
      } else {
        await(RegisterAndAwaitType(subjectProvider()))
      }
    }
  }

  /**
    * Retrieve the subjectType for the given typename
    * @param typeName name of the type
    * @return Future with the subjecttype (or nothing if not found)
    */
  def GetType(typeName: String): Future[SubjectType] = {
    val path = GetSubjectPath(typeName)
    zk(path).getData.apply().map(o => Deserialiser.Deserialize(o.bytes)).asScala
  }

  /**
    * Retrieves the current set of registered subject names
    * @return A future with the set of registered subjects
    */
  def GetSubjectNames(): Future[immutable.Set[String]] = {
    async {
      val zNode = await(zk(SubjectPath).getChildren.apply().asScala)
      zNode.children.map(o => o.name).toSet
    }
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
    val data = Serialiser.Serialize(subjectType)

    ZkUtil
      .Create(GetSubjectPath(subjectType.name), data)
      .flatMap(_ => ZkUtil.Create(GetStatePath(subjectType.name), GenericSerialiser(true)))
      .flatMap(_ => ZkUtil.Create(GetSinkPath(subjectType.name)))
      .flatMap(_ => ZkUtil.Create(GetSourcePath(subjectType.name)))
      .map(_ => subjectType)
      .recoverWith { //Register type. When error because node already exists just retrieve this value because the first writer wins.
        case _: NodeExistsException => GetType(subjectType.name)
      }
  }

  /**
    * Returns a future that contains the subjectType of the given name. Waits until the given type actually gets registered
    * @param typeName name of the type to find
    * @return future that will resolve when the given type has been found
    */
  def AwaitTypeRegistration(typeName: String): Future[SubjectType] = {
    //Make sure to create the offer before exists is called
    val watch = zk(SubjectPath).getChildren.watch()
    //This could cause unnessecary calls to Exists
    Exists(typeName).flatMap(o => {
      if (o) GetType(typeName)
      else
        watch.asScala.flatMap(o => o.update.asScala.flatMap(_ => AwaitTypeRegistration(typeName)))
    })
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
    val path = GetSinkPath(typeName, uuid)

    //Check if sink does not exist on type
    if (await(ZkUtil.pathExists(path))) {
      throw SinkAlreadySubscribedException(uuid.concat(" on ").concat(typeName))
    }

    await(ZkUtil.Create(path))
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
    val path = GetSourcePath(typeName, uuid)

    //Check if sink does not exist on type
    if (await(ZkUtil.pathExists(path))) {
      throw SourceAlreadySubscribedException(uuid.concat(" on ").concat(typeName))
    }

    await(ZkUtil.Create(path))
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
    val path = GetSinkPath(typeName, uuid)

    //Check if sink does not exist on type
    if (!await(ZkUtil.pathExists(path))) {
      throw SinkNotSubscribedException(uuid.concat(" on ").concat(typeName))
    }

    await(ZkUtil.Delete(path))
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
    val path = GetSourcePath(typeName, uuid)

    //Check if sink does not exist on type
    if (!await(ZkUtil.pathExists(path))) {
      throw SourceNotSubscribedException(uuid.concat(" on ").concat(typeName))
    }

    await(ZkUtil.Delete(path))
  }

  /**
    * Gets a list of uuids for all sinks that are subscribed on the type
    * @param typeName type to check for sinks
    * @throws TypeNameNotFoundException when typeName is not registered
    * @return A future that resolves with the registered sinks
    */
  def GetSinks(typeName: String): Future[Array[String]] = async {
    await(AssertExists(typeName))
    val path = GetSinkPath(typeName)
    await(zk(path).getChildren.apply().map(o => o.children.map(o => o.name).toArray).asScala)
  }

  /**
    * Gets a list of uuids for all sources that are subscribed on the type
    * @param typeName type to check for sources
    * @throws TypeNameNotFoundException when typeName is not registered
    * @return A future that resolves with the registered sources
    */
  def GetSources(typeName: String): Future[Array[String]] = async {
    await(AssertExists(typeName))
    val path = GetSourcePath(typeName)
    await(zk(path).getChildren.apply().map(o => o.children.map(o => o.name).toArray).asScala)
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
    * Un-register a subject from the library
    * TODO: Refactor this to some recursive delete
    * @throws ActiveSinkException when the subject still has an active sink
    * @throws ActiveSourceException when the subject still has an active source
    * @param name: String
    * @return A future that returns when the subject has actually been removed from the library
    */
  private[codefeedr] def UnRegisterSubject(name: String): Future[Boolean] = {
    logger.debug(s"Deleting type $name")

    val path = GetSubjectPath(name)
    Exists(name).flatMap(o =>
      if (o) {
        async {
          if (await(HasSinks(name))) throw ActiveSinkException(name)
          if (await(HasSources(name))) throw ActiveSourceException(name)

          await(ZkUtil.Delete(GetStatePath(name)))
          await(ZkUtil.Delete(GetSourcePath(name)))
          await(ZkUtil.Delete(GetSinkPath(name)))
          await(ZkUtil.Delete(GetSubjectPath(name)))
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
  def Exists(name: String): Future[Boolean] = ZkUtil.pathExists(GetSubjectPath(name))

  /**
    * Closes the subjectType
    * @param name name of the subject to close
    * @return a future that resolves when the write was succesful
    */
  def Close(name: String): Future[Unit] =
    zk(GetStatePath(name)).setData(GenericSerialiser(false), -1).asScala.map(_ => Unit)

  /**
    * Returns a future if the subject with the given name is still open
    * @param name name of the type
    * @return a future with boolean if the type was still open
    */
  def IsOpen(name: String): Future[Boolean] =
    zk(GetStatePath(name)).getData.apply().map(o => GenericDeserialiser[Boolean](o.bytes)).asScala

  /**
    * Constructs a future that resolves whenever the given type closes
    * @param name name of the type to wait for
    * @return A future that resolves when the type is closed
    */
  def awaitClose(name: String): Future[Unit] = async {
    //Make sure to create the offer before exists is called
    val watch = zk(GetStatePath(name)).getData.watch()
    //This could cause unnessecary calls to Exists
    IsOpen(name).flatMap(o => {
      if (!o) Future.successful()
      else watch.asScala.flatMap(o => o.update.asScala.flatMap(_ => awaitClose(name)))
    })
  }

}
