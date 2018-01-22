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

package org.codefeedr.Core.Library.Metastore

import com.typesafe.scalalogging.LazyLogging
import org.apache.zookeeper.KeeperException.NodeExistsException
import org.codefeedr.Core.Library.Internal.SubjectTypeFactory
import org.codefeedr.Core.Library.Internal.Zookeeper.{ZkClient, ZkNodeBase}
import org.codefeedr.Exceptions._
import org.codefeedr.Model.SubjectType

import scala.async.Async.{async, await}
import scala.collection.immutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.runtime.{universe => ru}

/**
  * ThreadSafe, Async
  * This class contains services to obtain data about subjects and kafka topics
  *
  * Created by Niels on 14/07/2017.
  */
class SubjectLibrary(val zk: ZkClient) extends LazyLogging {
  //Zookeeper path where the subjects are stored
  @transient private val SubjectPath = "/Codefeedr/Subjects"

  @transient implicit val zkClient: ZkClient = zk

  /**
    * Get the path to the zookeeper definition of the given subject
    *
    * @param s the name of the subject
    * @return the full path to the subject
    */
  private def GetSubjectNode(s: String): ZkNodeBase = ZkNode(s"$SubjectPath/$s")

  private def GetStateNode(s: String): ZkNodeBase = ZkNode(s"$SubjectPath/$s/state")

  private def GetSourceNode(s: String): ZkNodeBase = ZkNode(s"$SubjectPath/$s/source")

  private def GetSourceNode(s: String, uuid: String): ZkNodeBase =
    ZkNode(s"$SubjectPath/$s/source/$uuid")

  private def GetSinkNode(s: String): ZkNodeBase = ZkNode(s"$SubjectPath/$s/sink")

  private def GetSinkNode(s: String, uuid: String): ZkNodeBase = ZkNode(s"$SubjectPath/$s/sink/$uuid")

  /**
    * Initalisation method
    *
    * @return true when initialisation is done
    */
  def Initialize(): Future[Boolean] =
    ZkNode(SubjectPath).Create().map(_ => true)



  /**
    * Retrieves a subjecttype from the store if one is registered
    * Otherwise registeres the type in the store
    *
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
    * Returns a future that contains the subjectType of the given name. Waits until the given type actually gets registered
    *
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
    * Retrieve the subjectType for the given typename
    *
    * @param typeName name of the type
    * @return Future with the subjecttype (or nothing if not found)
    */
  def GetType(typeName: String): Future[Option[SubjectType]] =
    GetSubjectNode(typeName).GetData[SubjectType]()

  /**
    * Retrieves the current set of registered subject names
    *
    * @return A future with the set of registered subjects
    */
  def GetSubjectNames(): Future[immutable.Set[String]] = async {
    await(ZkNode(SubjectPath).GetChildren()).map(o => o.Name).toSet
  }

}
