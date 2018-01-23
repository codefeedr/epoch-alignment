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
import org.codefeedr.Core.Library.Internal.Zookeeper.{ZkClient, ZkNode, ZkNodeBase}
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
    * Initalisation method
    *
    * @return true when initialisation is done
    */
  def Initialize(): Future[Boolean] =
    ZkNode(SubjectPath).Create().map(_ => true)

  def GetSubjects(): SubjectCollectionNode = new SubjectCollectionNode(zk)
}
