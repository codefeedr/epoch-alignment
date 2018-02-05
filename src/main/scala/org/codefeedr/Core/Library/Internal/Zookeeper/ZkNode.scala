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

import com.typesafe.scalalogging.LazyLogging
import org.apache.zookeeper.KeeperException.NodeExistsException
import rx.lang.scala.Observable

import scala.async.Async.{async, await}
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * ZkNode, represents a node on zookeeper
  * Note that the node does not necessarily need to exist on zookeeper
  *
  * @param name name of the current node
  * @param parent the parent
  */
class ZkNode[TData: ClassTag](name: String, val parent: ZkNodeBase)
    extends ZkNodeBase(name)
    with LazyLogging {

  override def Parent(): ZkNodeBase = parent

  /**
    * Create a node with data
    *
    * @param data data to set on the created node
    * @return a future of the saved data
    */
  def Create(data: TData): Future[TData] = async {
    await(zkClient.CreateWithData(Path(), data))
    await(PostCreate())
    data
  }

  /**
    * Gets the data, or creates a node with the data
    *
    * @param factory    method that creates the data that should be stored in this zookeeper node
    * @return
    */
  def GetOrCreate(factory: () => TData): Future[TData] = {
    async {
      if (await(Exists())) {
        await(GetData()).get
      } else {
        val r = await(Create(factory()))
        //Call post-create hooks
        await(PostCreate())
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
  def GetData(): Future[Option[TData]] =
    zkClient.GetData[TData](Path())

  /**
    * Set data of the node
    *
    * @param data data object to set
    * @return a future that resolves when the data has been set
    */
  def SetData(data: TData): Future[Unit] = zkClient.SetData[TData](Path(), data).map(_ => Unit)

  /**
    * Create the child of the node with the given name
    *
    * @param name name of the child to create
    * @tparam TChild type exposed by the childnode
    * @return
    */
  def GetChild[TChild: ClassTag](name: String): ZkNode[TChild] = new ZkNode[TChild](name, this)

  /**
    * Creates a future that watches the node until the data matches the given condition
    *
    * @param condition Condition that should be true for the future to resolve
    * @return a future that resolves when the given condition is true
    */
  def AwaitCondition(condition: TData => Boolean): Future[TData] =
    zkClient.AwaitCondition(Path(), condition)

  /**
    * Get an observable of the data of the node
    * Note that it is not guaranteed this observable contains all events (or all modifications on zookeeper)
    * Just use this observable to keep track of the state, not to pass messages
    * For messages use Kafka
    * @return
    */
  def ObserveData(): Observable[TData] = zkClient.ObserveData[TData](Path())
}

object ZkNode {
  def apply[TData: ClassTag](name: String, parent: ZkNodeBase) = new ZkNode[TData](name, parent)
}
