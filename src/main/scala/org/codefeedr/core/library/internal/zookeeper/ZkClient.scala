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

import java.util

import scala.collection.JavaConverters._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.zookeeper.AsyncCallback._
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.Watcher.Event
import org.apache.zookeeper.Watcher.Event._
import org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE
import org.apache.zookeeper._
import org.apache.zookeeper.data.Stat
import org.codefeedr.core.library.internal.serialisation.{GenericDeserialiser, GenericSerialiser}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.async.Async._
import rx.lang.scala._
import rx.lang.scala.observables.{AsyncOnSubscribe, SyncOnSubscribe}

import scala.reflect.ClassTag

/**
  * ZkClient class
  * Took inspiration from https://github.com/bigtoast/async-zookeeper-client
  */
class ZkClient extends LazyLogging {
  @transient private lazy val conf: Config = ConfigFactory.load
  @transient private lazy val connectionString: String =
    conf.getString("codefeedr.zookeeper.connectionstring")
  @transient private lazy val connectTimeout =
    Duration(conf.getLong("codefeedr.zookeeper.connectTimeout"), SECONDS)
  @transient private lazy val sessionTimeout =
    Duration(conf.getLong("codefeedr.zookeeper.sessionTimeout"), SECONDS)

  @transient private lazy val connectPromise: Promise[Unit] = Promise[Unit]()

  @transient private var zk: ZooKeeper = _

  //A zkClient should always be connected
  //More clean solutions introduce a lot of complexity for very little performance gain
  Await.ready(connect(), connectTimeout)

  /**
    * Connect to the zookeeper server
    * If already connected, reconnects
    * @return a future that resolves when a connection has been made
    */
  private def connect(): Future[Unit] = {
    //If zookeeper already assigned first close existing connection
    if (zk != null) {
      close()
    }

    zk = new ZooKeeper(
      connectionString,
      sessionTimeout.toMillis.toInt,
      new Watcher {
        override def process(event: WatchedEvent): Unit = {
          event.getState match {
            case KeeperState.SyncConnected => connectPromise.completeWith(Create(""))
            case KeeperState.Expired => connect()
            case _ =>
          }
        }
      }
    )
    connectPromise.future
  }

  private def getDataCallback[T: ClassTag](subscriber: Subscriber[T]): DataCallback =
    new DataCallback {
      override def processResult(rc: Int,
                                 path: String,
                                 ctx: scala.Any,
                                 data: Array[Byte],
                                 stat: Stat): Unit = {
        Code.get(rc) match {
          case Code.OK => subscriber.onNext(GenericDeserialiser[T](data))
          case error => subscriber.onError(getError(error, path, stat, ctx))
        }
      }
    }

  /**
    * Prepends codefeedr to the zookeeper path so that no actual data outside the codefeedr path can be mutated (for example to destroy kafka)
    * @param s string to prepend
    * @return
    */
  private def prependPath(s: String) = s"/CodeFeedr$s"

  /**
    * Closes the connection to the zkClient
    */
  def close(): Unit = zk.close()

  /**
    * Returns the zookeeper connection object, for more advanced apis
    * Please only use when really necessary
    * @return
    */
  def getConnector(): ZooKeeper = zk

  /**
    * Get the raw bytearray at the specific node
    * @param path path to the node
    * @return a promise that resolves into the raw data
    */
  def getRawData(path: String,
                 ctx: Option[Any] = None,
                 watch: Option[Watcher] = None): Future[Array[Byte]] = {
    val resultPromise = Promise[Array[Byte]]
    zk.getData(
      prependPath(path),
      watch.orNull,
      new DataCallback {
        override def processResult(rc: Int,
                                   path: String,
                                   ctx: scala.Any,
                                   data: Array[Byte],
                                   stat: Stat): Unit = {
          handleResponse[Array[Byte]](resultPromise, rc, path, Option(ctx), data, stat)
        }
      },
      ctx
    )
    resultPromise.future
  }

  /**
    * Get the data of the node on the given path, and deserialize to the given generic parameter
    * @param path path of the node
    * @tparam T type to deserialize to
    * @return deserialized data
    */
  def getData[T: ClassTag](path: String): Future[Option[T]] =
    getRawData(path).map(o => if (o != null) Some(GenericDeserialiser[T](o)) else None)

  /**
    * Sets the data on the given node.
    * @param path path to the node to set data on. This path should already exist. To create a node use CreateWithData
    * @tparam T the object to serialise and set on the node
    * @return a future that resolves into the path to the node once the set has been completed
    */
  def setData[T: ClassTag](path: String, data: T, ctx: Option[Any] = None): Future[String] = {
    val resultPromise = Promise[String]
    zk.setData(
      prependPath(path),
      GenericSerialiser[T](data),
      -1,
      new StatCallback {
        override def processResult(rc: Int, path: String, c: scala.Any, stat: Stat): Unit = {
          handleResponse[String](resultPromise, rc, path, Some(c), path, stat)
        }
      },
      ctx
    )
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
  private def handleResponse[T](p: Promise[T],
                                rc: Int,
                                path: String,
                                ctx: Option[Any],
                                data: T,
                                stat: Stat = null): Unit = {
    Code.get(rc) match {
      case Code.OK => p.success(data)
      case error if path == null =>
        p.failure(
          ZkClientException(KeeperException.create(error), Option(path), Option(stat), ctx))
      case error =>
        p.failure(
          ZkClientException(KeeperException.create(error, path), Option(path), Option(stat), ctx))
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
  def createWithData[T: ClassTag](path: String, data: T, ctx: Option[Any] = None): Future[Unit] =
    Create(path, GenericSerialiser(data), ctx)

  /**
    * Recursively creates a path of nodes on zookeeper
    * @param path the path to the node to create
    * @return a future that resolves when the node has been created
    */
  def Create(path: String, data: Array[Byte] = null, ctx: Option[Any] = None): Future[Unit] =
    async {
      val p = path.lastIndexOf("/")
      if (p > 0) { //Both 0 and -1 should continue
        //Create child nodes if needed
        await(Create(path.take(p)))
      }
      val resultPromise = Promise[Unit]()
      zk.create(
        prependPath(path),
        data,
        OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT,
        new StringCallback {
          override def processResult(rc: Int, path: String, ignore: Any, name: String) {
            handleResponse[Unit](resultPromise, rc, path, ctx, Unit)
          }
        },
        ctx
      )
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
    zk.delete(
      prependPath(path),
      -1,
      new VoidCallback {
        override def processResult(rc: Int, path: String, ctx: Any): Unit = {
          handleResponse[Unit](resultPromise, rc, path, Some(ctx), Unit)
        }
      },
      ctx
    )
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
    zk.getChildren(
      prependPath(path),
      false,
      new ChildrenCallback {
        override def processResult(rc: Int,
                                   path: String,
                                   ctx: scala.Any,
                                   children: util.List[String]): Unit = {
          handleResponse[Iterable[String]](resultPromise, rc, path, Some(ctx), children.asScala)
        }
      },
      ctx
    )
    resultPromise.future
  }

  /**
    * Gets a recursive childwatcher that calls the callback whenever something changes on the children
    * @param p path to watch
    * @param cb callback to call
    * @return the watcher
    */
  def getRecursiveChildWatcher(p: String, cb: ChildrenCallback, cbDelete: () => Unit): Watcher =
    new Watcher {
      override def process(event: WatchedEvent): Unit = {
        event.getType match {
          case EventType.NodeDeleted => cbDelete()
          case EventType.NodeChildrenChanged =>
            zk.getChildren(p, getRecursiveChildWatcher(p, cb, cbDelete), cb, None)
          case _ => throw new Exception(s"Got unimplemented event: ${event.getType}")
        }
      }
    }

  /**
    * Gets a recursive data watcher that calls the callback whenever the data of the node modifies
    * @param p path to the node
    * @param subscriber the subscriber interested in the data
    * @return
    */
  def getRecursiveDataWatcher[T: ClassTag](p: String, subscriber: Subscriber[T]): Watcher = {
    new Watcher {
      override def process(event: WatchedEvent): Unit = {
        //Only process event if the subscriber is still interested
        if (!subscriber.isUnsubscribed) {
          event.getType match {
            case EventType.NodeDeleted => subscriber.onCompleted()
            case EventType.NodeDataChanged =>
              zk.getData(p,
                         getRecursiveDataWatcher(p, subscriber),
                         getDataCallback(subscriber),
                         None)
            case _ => throw new Exception(s"Got unimplemented event: ${event.getType}")
          }
        }
      }
    }
  }

  /**
    * Creates an observable that fires when something on the data of the given node changes
    * @param path path to the node which to observe
    * @tparam TData type of the data to observe
    * @return
    */
  def observeData[TData: ClassTag](path: String): Observable[TData] =
    Observable(subscriber => {
      val p = prependPath(path)
      val onComplete = () => subscriber.onCompleted()
      zk.getData(p, getRecursiveDataWatcher(p, subscriber), getDataCallback(subscriber), None)
    })

  /**
    * Get a zookeeper error
    * @param code zk error code
    * @param path path of the node
    * @param stat
    * @param ctx
    * @return
    */
  def getError(code: KeeperException.Code, path: String, stat: Stat, ctx: Any): ZkClientException =
    if (path == null) {
      ZkClientException(KeeperException.create(code), Option(path), Option(stat), Some(ctx))
    } else {
      ZkClientException(KeeperException.create(code, path), Option(path), Option(stat), Some(ctx))
    }

  /**
    * Creepy method name?
    * Place a watch on a node that will be called whenever a child is added or removed to the node
    * Note that the implementation does not guarantee the onNext is called for each single modification on the children.
    * Some modifications might be aggregated into a single onNext.
    * TODO: Does not check for any zookeeper errors yet
    * @param path parent node to watch on
    * @return observable that fires for notifications on the children
    */
  def observeChildren(path: String): Observable[Seq[String]] =
    Observable(subscriber => {
      val p = prependPath(path)
      val cb = new ChildrenCallback {
        override def processResult(rc: Int,
                                   path: String,
                                   ctx: scala.Any,
                                   children: util.List[String]): Unit = {
          subscriber.onNext(children.asScala)
        }
      }
      val deleteCb = () => subscriber.onCompleted()
      zk.getChildren(p, getRecursiveChildWatcher(p, cb, deleteCb), cb, None)
    })

  /**
    * Places an observable on the path that only produces events for new children added to the node of the given path
    * Maintains a mutable internal state
    * Current implementation does not guarantee an event is fired when a child is removed and added again
    * (but it should do so in all practical use cases)
    * Current implementation has the side effect of placing an subscription on a new instance of ObserveChildren
    * @param path the path to observe
    * @return
    */
  def observeNewChildren(path: String): Observable[String] =
    Observable(subscriber => {
      var previousState = Seq.empty[String]
      observeChildren(path).subscribe(
        o => {
          o.foreach(child => {
            if (!previousState.contains(child)) {
              subscriber.onNext(child)
            }
          })
          //Assign new list as current state
          //This way, if a child is removed and then later added an event is still fired
          previousState = o
        },
        error => subscriber.onError(error),
        () => subscriber.onCompleted()
      )
    })

  /**
    * Deletes a node and all its children
    * @param path the path to recursively delete
    * @return A future that resolves when the node has been deleted
    */
  def deleteRecursive(path: String): Future[Unit] = async {
    val p = if (path == "/") "" else path
    val children = await(GetChildren(p))
    await(Future.sequence(children.map(o => s"$p/$o").map(deleteRecursive)))
    await(Delete(path))
  }

  /**
    * Check if a path exists
    * TODO: Proper handling of errors entering from RC
    * @param path The path to check if it exists
    * @return
    */
  def exists(path: String, ctx: Option[Any] = None): Future[Boolean] = {
    val resultPromise = Promise[Boolean]
    zk.exists(
      prependPath(path),
      false,
      new StatCallback {
        override def processResult(rc: Int, path: String, ctx: scala.Any, stat: Stat): Unit = {
          if (stat == null) {
            resultPromise.success(false)
          } else {
            resultPromise.success(true)
          }
        }
      },
      ctx
    )
    resultPromise.future
  }

  /**
    * Creates a future that resolves when the node at the given path is removed
    * Future also resolves if the node does not exist
    * @param path path to the node to await removal
    * @return A future that resolves when the given node is removed
    */
  def awaitRemoval(path: String): Future[Unit] = {
    val promise = Promise[Unit]
    placeAwaitRemovalWatch(promise, path)
    promise.future
  }

  private def placeAwaitRemovalWatch(p: Promise[Unit], path: String): Unit = {
    zk.exists(
      prependPath(path),
      new Watcher {
        override def process(event: WatchedEvent): Unit = {
          if (!p.isCompleted) {
            if (event.getType == Event.EventType.NodeDeleted) {
              p.success()
            } else {
              placeAwaitRemovalWatch(p, path)
            }
          }
        }
      },
      new StatCallback {
        override def processResult(rc: Int, path: String, c: scala.Any, stat: Stat): Unit = {
          //If the code gets here, the node has been removed in between the firing and placing of the watch
          Code.get(rc) match {
            case Code.OK => ()
            case Code.NONODE => p.success(Unit)
            case error if path == null =>
              p.failure(
                ZkClientException(KeeperException.create(error),
                                  Option(path),
                                  Option(stat),
                                  Some(c)))
            case error =>
              p.failure(
                ZkClientException(KeeperException.create(error, path),
                                  Option(path),
                                  Option(stat),
                                  Some(c)))
          }
        }
      },
      None
    )
  }

  /**
    * Watches a nodes children until it actually has the given child
    * @param path path of the node to watch
    * @param child name of the child to wait for
    * @return a future that resolves when the node has a child with the given name
    */
  def awaitChild[T: ClassTag](path: String, child: String): Future[String] = {
    val promise = Promise[String]
    placeAwaitChildWatch(promise, path, child)
    promise.future
  }

  private def placeAwaitChildWatch(p: Promise[String], path: String, nemo: String): Unit = {
    zk.getChildren(
      prependPath(path),
      new Watcher {
        override def process(event: WatchedEvent): Unit = {
          if (!p.isCompleted) {
            if (event.getType == Event.EventType.NodeDeleted) {
              p.failure(NodeDeletedException(path))
            } else {
              placeAwaitChildWatch(p, path, nemo)
            }
          }
        }
      },
      new ChildrenCallback {
        override def processResult(rc: Int,
                                   path: String,
                                   ctx: scala.Any,
                                   children: util.List[String]): Unit = {
          //The promise could already be completed at this point by the previous trigger
          if (!p.isCompleted) {
            if (children != null && children.contains(nemo)) {
              handleResponse[String](p, rc, path, Some(ctx), nemo)
            }
            val code = Code.get(rc)
            code match {
              case Code.OK =>
              case error =>
                p.failure(
                  ZkClientException(KeeperException.create(error, path),
                                    Option(path),
                                    None,
                                    Some(ctx)))
            }
          }
        }
      },
      None
    )
  }

  /**
    * Creates a future that watches the zookeeper state until the given condition is true
    * @param path path to watch
    * @param condition condition function
    * @tparam T type of the node on the path
    * @return a future of the data of the watched node
    */
  def awaitCondition[T: ClassTag](path: String, condition: T => Boolean): Future[T] = {
    val promise = Promise[T]
    placeAwaitConditionWatch(promise, path, condition)
    promise.future
  }

  /**
    * Creates a readlock on the given path
    * Attempts to obtain lock from the moment of creation
    * Please use the returned object as managed resource
    * @param path path to the node for which to create the lock
    * @return readlock for the node
    */
  def readLock(path: String): Future[ZkReadLock] = ZkReadLock.open(prependPath(path))

  /**
    * Creates a writelock on the given path
    * Attempts to obtain lock from the moment of creation
    * Please use the returned object as managed resource to prevent deadlocks
    * @param path the path to the node for which to create the lock
    * @return writeLock for the node
    */
  def writeLock(path: String): Future[ZkWriteLock] = ZkWriteLock.open(prependPath(path))

  /**
    * Keeps placing watches on the given path until the given data condition is true
    * A watch might remain active after the promise has already been resolved
    * @param p promise to check on
    * @param path path to watch on
    * @param condition function that checks if the condition matches
    * @tparam T type of the data of the node
    */
  private def placeAwaitConditionWatch[T: ClassTag](p: Promise[T],
                                                    path: String,
                                                    condition: T => Boolean): Unit = {
    zk.getData(
      prependPath(path),
      new Watcher {
        override def process(event: WatchedEvent): Unit =
          if (!p.isCompleted) {
            placeAwaitConditionWatch(p, path, condition)
          }
      },
      new DataCallback {
        override def processResult(rc: Int,
                                   path: String,
                                   ctx: scala.Any,
                                   data: Array[Byte],
                                   stat: Stat): Unit = {
          //The promise could already be completed at this point by the previous trigger
          if (!p.isCompleted) {
            Code.get(rc) match {
              case Code.OK => {
                val serialised = GenericDeserialiser[T](data)
                if (condition(serialised)) {
                  p.success(serialised)
                }
              }
              case error if path == null =>
                p.failure(
                  ZkClientException(KeeperException.create(error),
                                    Option(path),
                                    Option(stat),
                                    Some(ctx)))
              case error =>
                p.failure(
                  ZkClientException(KeeperException.create(error, path),
                                    Option(path),
                                    Option(stat),
                                    Some(ctx)))
            }
          }
        }
      },
      None
    )
  }
}
