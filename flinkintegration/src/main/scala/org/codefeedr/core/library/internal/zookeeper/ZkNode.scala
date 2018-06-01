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

package org.codefeedr.core.library.internal.zookeeper

import com.typesafe.scalalogging.LazyLogging
import org.apache.zookeeper.KeeperException.NodeExistsException
import org.codefeedr.core.library.LibraryServices
import rx.lang.scala.Observable

import scala.async.Async.{async, await}
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * ZkNode, represents a node on zookeeper
  * Note that the node does not necessarily need to exist on zookeeper
  *
  * @param name name of the current node
  * @param p the parent
  */
class ZkNode[TData: ClassTag](name: String, val p: ZkNodeBase)
    extends ZkNodeBase(name)
    with PartialFunction[Unit, Future[TData]]
    with LazyLogging {

  override def parent(): ZkNodeBase = p

  /**
    * Create a node with data
    *
    * @param data data to set on the created node
    * @return a future of the saved data
    */
  def create(data: TData): Future[TData] = async {
    //TODO: Implement proper locks
    if (await(exists())) {
      if (!await(getData()).get.equals(data)) {
        throw new Exception(
          s"Cannot create node ${path()}. The node already exists with different data")
      }
    }
    await(zkClient.createWithData(path(), data))
    await(postCreate())
    data
  }

  /**
    * Gets the data, or creates a node with the data
    * @param factory method that creates the data that should be stored in this zookeeper node
    * @return
    */
  /*
  def getOrCreate(factory: () => TData): Future[TData] = {
    async {
      if (await(exists())) {
        await(getData()).get
      } else {
        val r = await(create(factory()))
        //Call post-create hooks
        await(postCreate())
        //Return data as result
        r
      }
    }.recoverWith { //Register type. When error because node already exists just retrieve this value because the first writer wins.
      case _: Exception => getData().map(o => o.get)
    }
  }
  */

  /**
    * Get the data of the current node
    *
    * @return
    */
  def getData(): Future[Option[TData]] =
    zkClient.getData[TData](path())

  /**
    * Retrieve the data with a blocking wait
    * @return
    */
  def getDataSync(): Option[TData] =
    Await.result(getData(), Duration(500, MILLISECONDS))

  /**
    * Set data of the node
    *
    * @param data data object to set
    * @return a future that resolves when the data has been set
    */
  def setData(data: TData): Future[Unit] = zkClient.setData[TData](path(), data).map(_ => Unit)

  /**
    * Create the child of the node with the given name
    *
    * @param name name of the child to create
    * @tparam TChild type exposed by the childnode
    * @return
    */
  def getChild[TChild: ClassTag](name: String): ZkNode[TChild] = new ZkNode[TChild](name, this)

  /**
    * Creates a future that watches the node until the data matches the given condition
    *
    * @param condition Condition that should be true for the future to resolve
    * @return a future that resolves when the given condition is true
    */
  def awaitCondition(condition: TData => Boolean): Future[TData] =
    zkClient.awaitCondition(path(), condition)

  /**
    * Get an observable of the data of the node
    * Note that it is not guaranteed this observable contains all events (or all modifications on zookeeper)
    * Just use this observable to keep track of the state, not to pass messages
    * For messages use Kafka
    * @return
    */
  def observeData(): Observable[TData] = zkClient.observeData[TData](path())

  override def isDefinedAt(x: Unit): Boolean = true

  override def apply(v1: Unit): Future[TData] = getData().map(o => o.get)
}

object ZkNode {
  def apply[TData: ClassTag](name: String, parent: ZkNodeBase) = new ZkNode[TData](name, parent)
}
