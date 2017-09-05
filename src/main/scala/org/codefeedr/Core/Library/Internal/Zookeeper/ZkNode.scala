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

package org.codefeedr.Core.Library.Internal.Zookeeper

import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * ZkNode, represents a node on zookeeper
  * Note that the node does not necessarily need to exist on zookeeper
  * @param path Path to the node on zookeeper
  */
class ZkNode(path: String) {

  /**
    * Name of the node
    */
  val Name: String = path.split('/').last

  /**
    * Creates the node on zookeeper
    * @return a future of the path used
    */
  def Create(): Future[String] = ZkClient().Create(path).map(_ => path)

  /**
    * Create a node with data
    * @param data data to set on the created node
    * @tparam T type of the data
    * @return a future of the used path
    */
  def Create[T: ClassTag](data: T): Future[String] =
    ZkClient().CreateWithData(path, data).map(_ => path)

  def GetData[T: ClassTag](): Future[Option[T]] = ZkClient().GetData[T](path)
  def GetChild(name: String): ZkNode = new ZkNode(s"$path/$name")
  def GetChildren(): Future[Iterable[ZkNode]] =
    ZkClient().GetChildren(path).map(o => o.map(p => new ZkNode(p)))

  /**
    * Creates a future that watches the node until the data matches the given condition
    * @param condition Condition that should be true for the future to resolve
    * @tparam T Type of the node
    * @return a future that resolves when the given condition is true
    */
  def AwaitCondition[T: ClassTag](condition: T => Boolean): Future[T] =
    ZkClient().AwaitCondition(path, condition)

  /**
    * Creates a future that awaits the registration of a specific child
    * @param child name of the child to await
    * @return a future that resolves when the child has been created, with the name of the child
    */
  def AwaitChild(child: String): Future[String] = ZkClient().AwaitChild(path, child)

  /**
    * Creates a future that resolves whenever the node has been deleted from zookeeper
    * @return the future
    */
  def AwaitRemoval(): Future[Unit] = ZkClient().AwaitRemoval(path)

  /**
    * Set data of the node
    * @param data data obejct to set
    * @tparam T Data to set
    * @return a future that resolves when the data has been set
    */
  def SetData[T: ClassTag](data: T): Future[Unit] =
    ZkClient().SetData[T](path, data).map(_ => Unit)

  /**
    * Checks if the node exists
    * @return a future with the result
    */
  def Exists(): Future[Boolean] = ZkClient().Exists(path)

  /**
    * Delete the current node
    * @return a future that resolves when the node has been deleted
    */
  def Delete(): Future[Unit] = ZkClient().Delete(path)

  /**
    * Delete the current node and all its children
    * @return a future that resolves when the node has been deleted
    */
  def DeleteRecursive(): Future[Unit] = ZkClient().DeleteRecursive(path)
}

object ZkNode {
  def apply(path: String) = new ZkNode(path)
}
