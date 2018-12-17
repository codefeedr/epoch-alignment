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
import org.codefeedr.core.library.internal.serialisation.GenericDeserialiser
import rx.lang.scala.Observable

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

trait ZkNode[TData] extends ZkNodeBase {

  def parent(): ZkNodeBase

  /**
    * Create a node with data
    *
    * @param data data to set on the created node
    * @return a future of the saved data
    */
  def create(data: TData): Future[TData]

  /**
    * Get the data of the current node
    *
    * @return
    */
  def getData(): Future[Option[TData]]

  /**
    * Retrieve the data with a blocking wait
    *
    * @return
    */
  def getDataSync(): Option[TData]

  /**
    * Set data of the node
    *
    * @param data data object to set
    * @return a future that resolves when the data has been set
    */
  def setData(data: TData): Future[Unit]

  /**
    * Create the child of the node with the given name
    *
    * @param name name of the child to create
    * @tparam TChild type exposed by the childnode
    * @return
    */
  def getChild[TChild: ClassTag](name: String): ZkNode[TChild]

  /**
    * Creates a future that watches the node until the data matches the given condition
    *
    * @param condition Condition that should be true for the future to resolve
    * @return a future that resolves when the given condition is true
    */
  def awaitCondition(condition: TData => Boolean): Future[TData]

  /**
    * Get an observable of the data of the node
    * Note that it is not guaranteed this observable contains all events (or all modifications on zookeeper)
    * Just use this observable to keep track of the state, not to pass messages
    * For messages use Kafka
    *
    * @return
    */
  def observeData(): Observable[TData]

  def isDefinedAt(x: Unit): Boolean

  def apply(v1: Unit): Future[TData]

  /**
    *  synchronize the current node and its children
    *  @return future when the synchronization has completed
    */
  def sync(): Future[Unit]
}

trait ZkNodeComponent extends ZkNodeBaseComponent { this: ZkClientComponent =>

  /**
    * ZkNode, represents a node on zookeeper
    * Note that the node does not necessarily need to exist on zookeeper
    *
    * @param name name of the current node
    * @param p    the parent
    */
  class ZkNodeImpl[TData: ClassTag](name: String, val p: ZkNodeBase)
      extends ZkNodeBaseImpl(name)
      with PartialFunction[Unit, Future[TData]]
      with LazyLogging
      with ZkNode[TData] {

    @transient lazy val deserializer: GenericDeserialiser[TData] = zkClient.getDeserializer[TData]

    override def parent(): ZkNodeBase = p

    /**
      * Create a node with data
      *
      * @param data data to set on the created node
      * @return a future of the saved data
      */
    override def create(data: TData): Future[TData] = async {
      //TODO: Implement proper locks
      logger.trace(s"Checking if node ${path()} exists")
      if (await(exists())) {
        logger.trace(s"Checking if data on node ${path()} is equal")
        if (!await(getData()).get.equals(data)) {
          throw new Exception(
            s"Cannot create node ${path()}. The node already exists with different data")
        }
      }
      logger.trace(s"Creating node with data on ${path()}")
      await(zkClient.createWithData(path(), data))
      logger.trace(s"Calling post-create on ${path()}")
      await(postCreate())
      logger.trace(s"Done calling post-create on ${path()}")
      data
    }

    /**
      * Get the data of the current node
      *
      * @return
      */
    override def getData(): Future[Option[TData]] =
      zkClient.getData[TData](path(), Some(deserializer))

    /**
      * Retrieve the data with a blocking wait
      *
      * @return
      */
    override def getDataSync(): Option[TData] = {
      logger.debug(s"Get data sync called on node with name $name")
      val r = zkClient.getDataSync[TData](path(), Some(deserializer))
      logger.debug(s"Got result of getdata on node with name $name")
      r
    }

    /**
      * Set data of the node
      *
      * @param data data object to set
      * @return a future that resolves when the data has been set
      */
    override def setData(data: TData): Future[Unit] =
      zkClient.setData[TData](path(), data).map(_ => Unit)

    /**
      * Create the child of the node with the given name
      *
      * @param name name of the child to create
      * @tparam TChild type exposed by the childnode
      * @return
      */
    override def getChild[TChild: ClassTag](name: String): ZkNode[TChild] =
      new ZkNodeImpl[TChild](name, this)

    /**
      * Creates a future that watches the node until the data matches the given condition
      *
      * @param condition Condition that should be true for the future to resolve
      * @return a future that resolves when the given condition is true
      */
    override def awaitCondition(condition: TData => Boolean): Future[TData] =
      zkClient.awaitCondition(path(), condition)

    /**
      * Get an observable of the data of the node
      * Note that it is not guaranteed this observable contains all events (or all modifications on zookeeper)
      * Just use this observable to keep track of the state, not to pass messages
      * For messages use Kafka
      *
      * @return
      */
    override def observeData(): Observable[TData] = zkClient.observeData[TData](path())

    override def isDefinedAt(x: Unit): Boolean = true

    override def apply(v1: Unit): Future[TData] = getData().map(o => o.get)

    override def sync(): Future[Unit] = zkClient.sync(path())
  }

  object ZkNode {
    def apply[TData: ClassTag](name: String, parent: ZkNodeBase)(implicit zkClient: ZkClient) =
      new ZkNodeImpl[TData](name, parent)
  }

}
