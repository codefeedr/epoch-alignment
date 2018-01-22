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

import org.apache.zookeeper.KeeperException.NodeExistsException

import scala.async.Async.{async, await}
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.concurrent.ExecutionContext.Implicits.global




/**
  * ZkNode, represents a node on zookeeper
  * Note that the node does not necessarily need to exist on zookeeper
  *
  * @param path Path to the node on zookeeper
  */
class ZkNode[TData: ClassTag](zkClient: ZkClient)(path: String) extends ZkNodeBase(zkClient)(path) {
  /**
    * Create a node with data
    *
    * @param data data to set on the created node
    * @return a future of the saved data
    */
  def Create(data: TData): Future[TData] =
    zkClient.CreateWithData(path, data).map(_ => data)

  /**
    * Gets the data, or creates a node with the data
    *
    * @param factory    method that creates the data that should be stored in this zookeeper node
    * @param postCreate actions to be performed after the node was created
    * @return
    */
  def GetOrCreate(factory:() => TData)(postCreate:() => Future[Unit]): Future[TData] = {
    async {
      if (await(Exists())) {
        await(GetData()).get
      } else {
        val r = await(Create(factory()))
        //Call post-create hooks
        await(postCreate())
        //Return data as result
        r
      }
    }.recoverWith { //Register type. When error because node already exists just retrieve this value because the first writer wins.
      case _: NodeExistsException => GetData().map(o => o.get)
    }
  }


  /**
    * Get the data of the current node
    *
    * @return
    */
  def GetData(): Future[Option[TData]] = zkClient.GetData[TData](path)

  /**
    * Create the child of the node with the given name
    *
    * @param name
    * @tparam TChild
    * @return
    */
  def GetChild[TChild: ClassTag](name: String): ZkNode[TChild] = new ZkNode[TChild](zkClient)(s"$path/$name")


  /**
    * Creates a future that watches the node until the data matches the given condition
    *
    * @param condition Condition that should be true for the future to resolve
    * @return a future that resolves when the given condition is true
    */
  def AwaitCondition(condition: TData => Boolean): Future[TData] =
    zkClient.AwaitCondition(path, condition)


  /**
    * Set data of the node
    *
    * @param data data obejct to set
    * @return a future that resolves when the data has been set
    */
  def SetData(data: TData): Future[Unit] = zkClient.SetData[TData](path, data).map(_ => Unit)


}

object ZkNode {
  def apply[TData: ClassTag](path: String)(implicit zkClient: ZkClient) = new ZkNode[TData](zkClient)(path)
}
