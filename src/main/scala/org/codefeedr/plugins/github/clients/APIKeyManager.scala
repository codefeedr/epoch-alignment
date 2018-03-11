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
package org.codefeedr.plugins.github.clients

import com.typesafe.config._
import org.codefeedr.core.library.LibraryServices
import org.codefeedr.core.library.internal.zookeeper.ZkNode
import org.slf4j.{Logger, LoggerFactory}

import async.Async._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.collection.JavaConverters._
import resource._

import scala.concurrent.duration.Duration

/**
  * Case class that represents an API key.
  * @param key the actual key.
  * @param requestLimit the request limit on this key.
  * @param requestsLeft the requests left on this key.
  * @param resetTime the time on which the request limit should be reset.
  * @param available if the key is available to use or not.
  */
case class APIKey(val key: String,
                  val requestLimit: Int,
                  val requestsLeft: Int,
                  var resetTime: Long = 0,
                  available: Boolean = true)

/**
  * Manages all API keys in ZooKeeper.
  */
class APIKeyManager {

  //TODO: Handle keys which have to reset (due to time limit)

  //default logger
  private lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  private lazy val config: Config = ConfigFactory.load()
  private lazy val zkClient = LibraryServices.zkClient

  //node under which the keys are stored
  private lazy val keysNode = new KeysNode

  /**
    * Loads all the keys from the configuration.
    * @return a list of API keys.
    */
  def loadKeys(): List[APIKey] = {
    val apiList = config
      .getObjectList("codefeedr.input.github.keys")
      .asScala
      .map(
        x =>
          new APIKey(x.toConfig.getString("key"),
                     x.toConfig.getInt("limit"),
                     x.toConfig.getInt("limit")))

    apiList.toList
  }

  /**
    * Saves all keys from the configuration to ZooKeeper.
    * @return all created keys.
    */
  def saveToZK(): Future[List[APIKey]] = async {
    val exists = await(keysNode.exists())

    //create if it doesn't exist yet
    if (!exists) {
      await(keysNode.create())
    }

    //get or create key
    val createKeys = Future.sequence(loadKeys().map { x =>
      val node = new ZkNode[APIKey](x.key, keysNode)
      node.getOrCreate(() => x)
    })

    //await till done
    await(createKeys)
  }

  /**
    * Gets an available key with the most requests left.
    * If no key is available None will be returned.
    * @return a key or None if no key available.
    */
  def acquireKey(): Future[Option[APIKey]] = async {
    var keyFound = false //key is not found yet

    val keyNodes = await(getKeyNodes()) //get all key nodes
    val keyData = await(Future.sequence(keyNodes.map(x => x.getData()))) //get all key data

    await(resetKeys(keyData.map(_.get))) //reset keys if the can be

    var keysAvailable = keyData
      .map(_.get)
      .filter(_.available) //check if available
      .filter(_.requestsLeft > 0) //remove keys with no requests left
      .sortWith(_.requestsLeft > _.requestsLeft) //sort on highest requests left

    //set currentKey to None
    var returnKey: Option[APIKey] = None

    //keep looking for an available key until none is left
    while (!keyFound && keysAvailable.size > 0) {
      val firstKey = keysAvailable.head //get key with most requests left

      //try to acquire this key by looking it
      val acquireKey = await(checkKey(new ZkNode[APIKey](s"${firstKey.key}", keysNode)))
      keyFound = acquireKey._1 //will be true if the key is successfully acquired
      keysAvailable = keysAvailable.filter(_.key != firstKey.key) //remove it from the list

      //set returnKey if it is found
      returnKey = if (keyFound) Some(acquireKey._2) else None
    }

    //return key if found (or not).
    returnKey
  }

  /**
    * Get all keynodes under the /Keys path.
    * @return all ZkNodes found.
    */
  private def getKeyNodes(): Future[List[ZkNode[APIKey]]] = async {
    //get all children
    val children = await(zkClient.GetChildren(keysNode.path())).toList

    //get all keys
    children.map(key => new ZkNode[APIKey](s"$key", keysNode))
  }

  /**
    * Tries to check and acquire key using a WriteLock.
    * @param key the key to acquire.
    * @return (true, key) if the key is successfully locked, (false, key) if not.
    */
  private def checkKey(key: ZkNode[APIKey]): Future[(Boolean, APIKey)] = async {
    val lock = await(key.writeLock()) //await the write lock

    managed(lock).acquireAndGet { x =>
      var data: APIKey = Await.result(key.getData(), Duration.Inf).get //get the key

      if (data.available) { //if available
        data = data.copy(available = false) //set unavailable
        Await.result(key.setData(data), Duration.Inf)
        (true, data)
      } else {
        (false, data)
      }
    }

  }

  /**
    * Updates the key with a new request limit and makes the key available again.
    * @param key the key to update.
    */
  def updateAndReleaseKey(key: APIKey) = async {
    val node = new ZkNode[APIKey](key.key, keysNode)
    val lock = await(node.setData(key.copy(available = true)))
  }

  /**
    * Resets keys for which the resetTime is expired and 0 requests left.
    * @param keys all the keys to check.
    */
  private def resetKeys(keys: List[APIKey]) = async {
    val currentTime = System.currentTimeMillis()
    val keysToReset = keys
      .filter(x => x.requestsLeft == 0)
      . //we only consider keys without any requests left
      filter(x => x.resetTime <= currentTime) //check if the reset time is before the current time

    val resettedKeys = keysToReset
      .map { x =>
        val newTime = x.resetTime + 3600000

        x.copy(requestsLeft = x.requestLimit, resetTime = newTime) //reset limit and time
      }
      .map(resetKey(_)) //map to their future

    await(Future.sequence(resettedKeys)) //wait till all reset
  }

  /**
    * Resets a key. (Within this method it is checked if the key is already
    * @param key the updated key.
    */
  private def resetKey(key: APIKey) = async {
    val node = new ZkNode[APIKey](key.key, keysNode)
    val lock = await(node.writeLock()) //get a write lock

    managed(lock).acquireAndGet { x =>
      var data: APIKey = Await.result(node.getData(), Duration.Inf).get //get the key once again (it might be updated already)

      if (data.requestsLeft == 0 && data.resetTime != key.resetTime) { //check if the key is already updated
        Await.result(node.setData(key), Duration.Inf) //update key
      }
    }
  }

}
