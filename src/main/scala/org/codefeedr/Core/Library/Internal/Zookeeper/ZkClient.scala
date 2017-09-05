

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

import scala.collection.JavaConverters._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.zookeeper.AsyncCallback._
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.Watcher.Event
import org.apache.zookeeper.Watcher.Event._
import org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE
import org.apache.zookeeper._
import org.apache.zookeeper.data.Stat
import org.codefeedr.Core.Library.Internal.Serialisation.{GenericDeserialiser, GenericSerialiser}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.async.Async._

import scala.reflect.ClassTag


/**
  * ZkClient class
  * Took inspiration from https://github.com/bigtoast/async-zookeeper-client
  */
class ZkClient {
  @transient private lazy val conf: Config = ConfigFactory.load
  @transient private lazy val connectionString: String = conf.getString("codefeedr.zookeeper.connectionstring")
  @transient private lazy val connectTimeout = Duration(conf.getLong("codefeedr.zookeeper.connectTimeout"), SECONDS)
  @transient private lazy val sessionTimeout = Duration(conf.getLong("codefeedr.zookeeper.sessionTimeout"), SECONDS)

  @transient private lazy val connectPromise: Promise[Unit] = Promise[Unit]()

  @transient private var zk: ZooKeeper = _

  //A zkClient should always be connected
  //More clean solutions introduce a lot of complexity for very little performance gain
  Await.ready(Connect(), connectTimeout)


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
            event.getState match {
            case KeeperState.SyncConnected => connectPromise.success()
            case KeeperState.Expired => Connect()
            case _ =>
          }
        }
    })
    connectPromise.future
  }

  def Close(): Unit = zk.close()

  /**
    * Get the raw bytearray at the specific node
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

  /**
    * Get the data of the node on the given path, and deserialize to the given generic parameter
    * @param path path of the node
    * @tparam T type to deserialize to
    * @return deserialized data
    */
  def GetData[T: ClassTag](path: String): Future[T] = GetRawData(path).map(GenericDeserialiser[T])

  /**
    * Sets the data on the given node.
    * @param path path to the node to set data on. This path should already exist. To create a node use CreateWithData
    * @tparam T the object to serialise and set on the node
    * @return a future that resolves into the path to the node once the set has been completed
    */
  def SetData[T: ClassTag](path: String, data: T, ctx: Option[Any] = None): Future[String] = {
    val resultPromise = Promise[String]
    zk.setData(path, GenericSerialiser[T](data),-1, new StatCallback {
      override def processResult(rc: Int, path: String, c: scala.Any, stat: Stat): Unit = {
        HandleResponse[String](resultPromise, rc, path, Some(c), path, stat)
      }
    },ctx)
    resultPromise.future
  }

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
  private def HandleResponse[T](p: Promise[T], rc: Int, path: String, ctx: Option[Any], data: T, stat: Stat = null): Unit = {
    Code.get(rc) match {
      case Code.OK => p.success(data)
      case error if path == null => p.failure(ZkClientException(KeeperException.create(error), Option(path), Option(stat),ctx))
      case error => p.failure(ZkClientException(KeeperException.create(error, path), Option(path), Option(stat),ctx))
    }
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
        HandleResponse[Unit](resultPromise, rc, path,ctx,Unit)
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
        HandleResponse[Unit](resultPromise, rc, path, Some(ctx),Unit)
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
  def GetChildren(path: String, ctx: Option[Any] = None): Future[Iterable[String]] = {
    val resultPromise = Promise[Iterable[String]]
    zk.getChildren(path,false,new ChildrenCallback {
      override def processResult(rc: Int, path: String, ctx: scala.Any, children: util.List[String]): Unit = {
        HandleResponse[Iterable[String]](resultPromise,rc,path,Some(ctx),children.asScala)
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
    * Creates a future that resolves when the node at the given path is removed
    * Future also resolves if the node does not exist
    * @param path path to the node to await removal
    * @return A future that resolves when the given node is removed
    */
  def AwaitRemoval(path: String): Future[Unit] = {
    val promise = Promise[Unit]
    PlaceAwaitRemovalWatch(promise, path)
    promise.future
  }

  private def PlaceAwaitRemovalWatch(p:Promise[Unit], path:String): Unit = {
    zk.exists(path, new Watcher {
      override def process(event: WatchedEvent): Unit = {
        if (!p.isCompleted) {
          if (event.getType == Event.EventType.NodeDeleted) {
            p.success()
          } else {
            PlaceAwaitRemovalWatch(p, path)
          }
        }
      }
    },new StatCallback {
      override def processResult(rc: Int, path: String, c: scala.Any, stat: Stat): Unit = {
        //If the code gets here, the node has been removed in between the firing and placing of the watch
        if(!p.isCompleted && stat==null) {
          HandleResponse[Unit](p, rc, path, Some(c), Unit, stat)
        }
      }
    },None)
  }



  /**
    * Watches a nodes children until it actually has the given child
    * @param path path of the node to watch
    * @param child name of the child to wait for
    * @return a future that resolves when the node has a child with the given name
    */
  def AwaitChild[T:ClassTag](path: String, child: String): Future[String] = {
    val promise = Promise[String]
    PlaceAwaitChildWatch(promise, path, child)
    promise.future
  }

  private def PlaceAwaitChildWatch(p: Promise[String], path: String, nemo: String): Unit = {
    zk.getChildren(path, new Watcher {
      override def process(event: WatchedEvent): Unit = {
        if(!p.isCompleted) {
          if(event.getType == Event.EventType.NodeDeleted) {
            p.failure(NodeDeletedException(path))
          } else {
            PlaceAwaitChildWatch(p, path, nemo)
          }
        }
      }
    }, new ChildrenCallback {
      override def processResult(rc: Int, path: String, ctx: scala.Any, children: util.List[String]): Unit = {
        //The promise could already be completed at this point by the previous trigger
        if(!p.isCompleted) {
          if (children.contains(nemo)) {
            HandleResponse[String](p, rc, path, Some(ctx),nemo)
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
        if(!p.isCompleted) {
          PlaceAwaitConditionWatch(p, path, condition)
        }
    }, new DataCallback {
      override def processResult(rc: Int, path: String, ctx: scala.Any, data: Array[Byte], stat: Stat): Unit = {
        //The promise could already be completed at this point by the previous trigger
        if(!p.isCompleted) {
          val serialised = GenericDeserialiser[T](data)
          if (condition(serialised)) {
            HandleResponse[T](p, rc, path, Some(ctx), serialised)
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
}


object ZkClient {
  @transient lazy val client: ZkClient = new ZkClient()
  def apply(): ZkClient = client

}