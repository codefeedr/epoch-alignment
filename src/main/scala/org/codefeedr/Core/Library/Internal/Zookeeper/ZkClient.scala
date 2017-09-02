

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

import java.util

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.zookeeper.AsyncCallback._
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.Watcher.Event._
import org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE
import org.apache.zookeeper._
import org.apache.zookeeper.data.Stat
import org.codefeedr.Core.Library.Internal.Serialisation.{GenericDeserialiser, GenericSerialiser}

import scala.concurrent._
import scala.concurrent.duration._
import scala.async.Async.{async, await}
import scala.reflect.ClassTag


/**
  * ZkClient class
  * Took inspiration from https://github.com/bigtoast/async-zookeeper-client
  */
object ZkClient {
  @transient lazy val conf: Config = ConfigFactory.load
  @transient lazy val connectionString: String = conf.getString("codefeedr.zookeeper.connectionstring")
  @transient lazy val connectTimeout = Duration(conf.getLong("codefeedr.zookeeper.connectTimeout"), SECONDS)
  @transient lazy val sessionTimeout = Duration(conf.getLong("codefeedr.zookeeper.sessionTimeout"), SECONDS)

  @transient lazy val assignPromise: Promise[Unit] = Promise[Unit]()
  @transient lazy val connectPromise: Promise[Unit] = Promise[Unit]()

  @transient private var zk: ZooKeeper = _

  /**
    * Connect to the zookeeper server
    * If already connected, reconnects
    * @return a future that resolves when a connection has been made
    */
  def Connect(): Future[Unit] = {
    //If zookeeper already assigned first close existing connection
    if(zk != null) {
      Close()
    }
    zk = new ZooKeeper(connectionString, sessionTimeout.toMillis.toInt, new Watcher {
      override def process(event: WatchedEvent): Unit = {
        assignPromise.future onSuccess
          {case _ => event.getState match {
            case KeeperState.SyncConnected => connectPromise.success()
            case KeeperState.Expired => Connect()
            case _ =>
          }}
      }
    })
    connectPromise.future
  }

  def Close(): Unit = zk.close()

  /**
    * Get the raw bytedarray at the specific node
    * @param path path to the node
    * @return a promise that resolves into the raw data
    */
  def GetRawData(path: String, ctx: Option[Any] = None, watch: Option[Watcher] = None): Future[Array[Byte]] = {
    val resultPromise = Promise[Array[Byte]]
    zk.getData(path,watch.orNull,new DataCallback {
      override def processResult(rc: Int, path: String, ctx: scala.Any, data: Array[Byte], stat: Stat): Unit = {
        HandleResponse[Array[Byte]](resultPromise, rc, path, Option(ctx),data, stat)
      }
    }, ctx)
    resultPromise.future
  }

  def GetData[T: ClassTag](path: String): Future[T] = GetRawData(path).map(GenericDeserialiser[T])

  /**
    * Internal method used to satisfy the result promise, and perform custom error handling
    * @param p promise to set the result on
    * @param rc State
    * @param path path related to the result
    * @param ctx context
    * @param data data to complete the promise with
    * @param stat Zookeeper stat object
    * @tparam T type of the data and promise
    */
  private def HandleResponse[T](p: Promise[T], rc: Int, path: String, ctx: Option[Any], data: T = null, stat: Stat = null): Unit = {
    Code.get(rc) match {
      case Code.OK => p.success(data)
      case error if path == null => p.failure(ZkClientException(KeeperException.create(error), Option(path), Option(stat),ctx))
      case error => p.failure(ZkClientException(KeeperException.create(error, path), Option(path), Option(stat),ctx))
    }
  }

  private def HandleEvent[T](p: Promise[T], watchedEvent: WatchedEvent, handler: (Promise[T], WatchedEvent) => Unit): Unit = {

  }

  /**
    * Creates a node with the given data (serialises data to byte array)
    * @param path path to node to create
    * @param data data to set on the node
    * @param ctx context
    * @tparam T type of the data
    * @return
    */
  def CreateWithData[T: ClassTag](path: String, data: T, ctx: Option[Any] = None): Future[Unit] = Create(path, GenericSerialiser(data),ctx)

  /**
    * Recursively creates a path of nodes on zookeeper
    * @param path the path to the node to create
    * @return a future that resolves when the node has been created
    */
  def Create(path: String, data: Array[Byte] = null, ctx: Option[Any] = None): Future[Unit] = async {
    val p = path.lastIndexOf("/")
    if(p != 0) {
      //Create child nodes if needed
      await(Create(path.take(p)))
    }
    val resultPromise = Promise[Unit]()
    zk.create(path, data,OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,  new StringCallback {
      override def processResult(rc: Int, path: String, ignore: Any, name: String) {
        HandleResponse(resultPromise, rc, path,ctx)
      }
    },ctx)
      resultPromise.future
  }

  /**
    * Delete a path. Throws an exception if the path does not exist or has children
    * @param path the path to delete
    * @param ctx context
    * @return
    */
  def Delete(path: String, ctx: Option[Any] = None): Future[Unit] = {
    val resultPromise = Promise[Unit]
    zk.delete(path,-1, new VoidCallback {
      override def processResult(rc: Int, path: String, ctx: Any): Unit = {
        HandleResponse(resultPromise, rc, path, Some(ctx))
      }
    },ctx)
    resultPromise.future
  }

  /**
    * Retrieve the children of the given node
    * @param path path to the node to retrieve children from
    * @param ctx context
    * @return An array of full paths to the children
    */
  def GetChildren(path: String, ctx: Option[Any] = None): Future[List[String]] = {
    val resultPromise = Promise[List[String]]
    zk.getChildren(path,false,new ChildrenCallback {
      override def processResult(rc: Int, path: String, ctx: scala.Any, children: util.List[String]): Unit = {
        HandleResponse(resultPromise,rc,path,Some(ctx),children)
      }
    },ctx)
    resultPromise.future
  }

  /**
    * Deletes a node and all its children
    * @param path the path to recursively delete
    * @return A future that resolves when the node has been deleted
    */
  def DeleteRecursive(path: String): Future[Unit] = async {
    val children = await(GetChildren(path))
    await(Future.sequence(children.map(DeleteRecursive)))
    await(Delete(path))
  }

  /**
    * Check if a path exists
    * @param path The path to check if it exists
    * @return
    */
  def Exists(path: String, ctx: Option[Any] = None): Future[Boolean] = {
    val resultPromise = Promise[Boolean]
    zk.exists(path,false, new StatCallback {
      override def processResult(rc: Int, path: String, ctx: scala.Any, stat: Stat): Unit = {
        if(stat == null) {
          HandleResponse(resultPromise,rc,path, Some(ctx),false)
        } else {
          HandleResponse(resultPromise,rc,path, Some(ctx),true)
        }
      }
    }, ctx)
    resultPromise.future
  }

  /**
    * Watches a nodes children until it actually has the given child
    * @param path path of the node to watch
    * @param child name of the child to wait for
    * @return a future that resolves when the node has a child with the given name
    */
  def AwaitChild[T:ClassTag](path: String, child: String): Future[Unit] = {
    val promise = Promise[Unit]
    PlaceAwaitChildWatch(promise, path, child)
    promise.future
  }

  private def PlaceAwaitChildWatch(p: Promise[Unit], path: String, nemo: String): Unit = {
    zk.getChildren(path, new Watcher {
      override def process(event: WatchedEvent): Unit = {
        HandleEvent(p, event, (promise, _) => {
          if(!promise.isCompleted) {
            PlaceAwaitChildWatch(p, path, nemo)
          }
        })
      }
    }, new ChildrenCallback {
      override def processResult(rc: Int, path: String, ctx: scala.Any, children: util.List[String]): Unit = {
        //The promise could already be completed at this point by the previous trigger
        if(!p.isCompleted) {
          if (children.contains(nemo)) {
            HandleResponse(p, rc, path, Some(ctx))
          }
        }
      }
    }, None)
  }

  /**
    * Creates a future that watches the zookeeper state until the given condition is true
    * @param path path to watch
    * @param condition condition function
    * @tparam T type of the node on the path
    * @return a future of the data of the watched node
    */
  def AwaitCondition[T:ClassTag](path: String, condition: T => Boolean): Future[T] = {
    val promise = Promise[T]
    PlaceAwaitConditionWatch(promise, path, condition)
    promise.future
  }

  /**
    * Keeps placing watches on the given path until the given data condition is true
    * A watch might remain active after the promsie has already been resolved
    * @param p promise to check on
    * @param path path to watch on
    * @param condition function that checks if the condition matches
    * @tparam T type of the data of the node
    */
  private def PlaceAwaitConditionWatch[T:ClassTag](p: Promise[T], path: String, condition: T => Boolean): Unit = {
    zk.getData(path,new Watcher {
      override def process(event: WatchedEvent): Unit =
        HandleEvent(p, event, (promise, _) => {
          if(!promise.isCompleted) {
            PlaceAwaitConditionWatch(p, path, condition)
          }
        })

    }, new DataCallback {
      override def processResult(rc: Int, path: String, ctx: scala.Any, data: Array[Byte], stat: Stat): Unit = {
        //The promise could already be completed at this point by the previous trigger
        if(!p.isCompleted) {
          val data = GenericDeserialiser[T](data)
          if (condition(data)) {
          HandleResponse(p, rc, path, Some(ctx), data)
          }
        }
      }
    }, None)
  }


  /**
    * @return a node representing the root
    */
  def GetRoot(): ZkNode = new ZkNode("/")

  /**
    * @param path absolute path to the node to retrieve
    * @return a reference to a node in the zookeeper store
    */
  def GetNode(path: String): ZkNode = new ZkNode(path)

  //Make sure that all clients are connected when they are constructed
  //TODO: Do we want to construct the zookeeper client properly asynchronous aswel?
  //Currently it is a singleton, so seems to add a lot of complexity for little gain
    Await.ready(Connect(), connectTimeout)
}