

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

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.typesafe.config.{Config, ConfigFactory}

import org.apache.zookeeper.Watcher.Event._
import org.apache.zookeeper._

import scala.concurrent._
import scala.concurrent.duration._
import scala.async.Async.{async, await}


/**
  * ZkClient class
  * Took inspiration from https://github.com/bigtoast/async-zookeeper-client
  */
class ZkClient {
  @transient lazy val conf: Config = ConfigFactory.load
  @transient lazy val connectionString: String = conf.getString("codefeedr.zookeeper.connectionstring")
  @transient lazy val connectTimeout = Duration(conf.getLong("codefeedr.zookeeper.connectTimeout"), SECONDS)
  @transient lazy val sessionTimeout = Duration(conf.getLong("codefeedr.zookeeper.sessionTimeout"), SECONDS)

  @transient lazy val assignPromise = Promise[Unit]()
  @transient lazy val connectPromise = Promise[Unit]()

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


}


object ZkClient {

}