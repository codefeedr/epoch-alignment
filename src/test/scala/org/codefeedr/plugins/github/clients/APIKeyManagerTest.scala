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

import org.codefeedr.core.LibraryServiceSpec
import org.codefeedr.core.library.internal.zookeeper.{ZkNode, ZkNodeBase}
import org.scalatest.BeforeAndAfterEach

import scala.async.Async._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class APIKeyManagerTest extends LibraryServiceSpec  with BeforeAndAfterEach {

  val keyNode = new KeysNode
  val keyManager = new APIKeyManager()

  override def beforeEach() : Unit = {
    Await.ready(keyNode.deleteRecursive(), Duration(1, SECONDS))
    Await.result(keyManager.saveToZK(), Duration(1, SECONDS))
  }
  override def afterEach() : Unit = {
    Await.ready(keyNode.deleteRecursive(), Duration(1, SECONDS))
  }

  "The APIKeyManager" should "correctly store all keys in ZooKeeper" in async {
    val children = await(zkClient.GetChildren("/keys")).toList

    assert(children.size == keyManager.loadKeys().size)
    assert(children == keyManager.loadKeys().map(_.key).reverse)
  }

  "The APIKeyManager" should "lock a key when a key is requested" in async {
    val keyLocked = await(keyManager.acquireKey())

    val childrenData = await(getChildrenData())

    val foundKey = childrenData.find(x => x.get.key == keyLocked.get.key).get
    assert(foundKey.get.available == false) //foundKey should be locked
  }

  "The APIKeyManager" should "lock multiple keys when multiple requests are done" in async {
    val calls = keyManager.acquireKey() :: keyManager.acquireKey() :: keyManager.acquireKey() :: Nil
    val keysLocked = await(Future.sequence(calls))

    val childrenData = await(getChildrenData())

    assert(childrenData.filter(!_.get.available).size == 2) //2 keys are locked
    assert(keysLocked.filter(_.isEmpty).size == 1) //1 getKey called returned None
    assert(keysLocked.filter(!_.isEmpty).size == 2) //2 getKeys called returned an actual key
  }

  "The APIKeyManager" should "unlock a key after it has been released" in async {
    val keyLocked = await(acquireAndReleaseKey(2))

    val childrenData = await(getChildrenData())

    val foundKey = childrenData.find(x => x.get.key == keyLocked.key).get
    assert(foundKey.get.available == true) //foundKey should be available again
    assert(foundKey.get.requestsLeft == 2) //requests left should be equal to zero
  }

  "The APIKeyManager" should "resets a key after the reset time (synchronous requests)" in async {
    val firstKey = await(acquireAndReleaseKey(0)) //this one should be reset afterwards because resetTime is 0
    val secondKey = await(acquireAndReleaseKey(0)) //firstKey should now be reset and secondKey is 0

    val childrenData = await(getChildrenData())
    val firstKeyFound = childrenData.find(x => x.get.key == firstKey.key).get
    val secondKeyFound = childrenData.find(x => x.get.key == secondKey.key).get

    //both are available
    assert(firstKeyFound.get.available == true)
    assert(secondKeyFound.get.available == true)

    //first key is reset
    assert(firstKeyFound.get.requestsLeft == firstKeyFound.get.requestLimit)
    assert(firstKeyFound.get.resetTime != 0)

    //second key has to be reset
    assert(secondKeyFound.get.requestsLeft == 0)
  }

  "The APIKeyManager" should "resets a key after the reset time (concurrent requests) " in async {
    val firstKey = acquireAndReleaseKey(0) //both requests are done at the same time
    val secondKey = acquireAndReleaseKey(0)

    val keys = await(Future.sequence(firstKey :: secondKey :: Nil))

    val childrenData = await(getChildrenData())

    val firstKeyFound = childrenData.find(x => x.get.key == keys(0).key).get
    val secondKeyFound = childrenData.find(x => x.get.key == keys(1).key).get

    //both are available
    assert(firstKeyFound.get.available == true)
    assert(secondKeyFound.get.available == true)

    //both keys are not reset
    assert(firstKeyFound.get.requestsLeft == 0)
    assert(secondKeyFound.get.requestsLeft == 0)

    //if we do a third request, both keys should be resetted
    val thirdRequest = await(acquireAndReleaseKey(10))

    val childrenDataUpdated = await(getChildrenData())

    //get third key
    val thirdKeyFound = childrenDataUpdated.find(x => x.get.key == thirdRequest.key).get

    assert(childrenDataUpdated.filter(x => x.get.requestsLeft > 0).size == 2) //2 keys with > 0 requests left
    assert(thirdKeyFound.get.requestsLeft == 10)
  }

  /**
    * Acquires a key and releases it with updated requests left.
    * @param newRequests the new amount of requests left.
    * @return the updated key.
    */
  def acquireAndReleaseKey(newRequests: Int) : Future[APIKey] = async {
    val key = await(keyManager.acquireKey()).get
    val updatedKey = key.copy(requestsLeft = newRequests)
    await(keyManager.updateAndReleaseKey(updatedKey))

    updatedKey
  }

  /**
    * Get the current data stored in ZK about the keys.
    * @return all children data.
    */
  def getChildrenData() : Future[List[Option[APIKey]]] = async {
    val children = await(zkClient.GetChildren("/keys")).toList
    await(Future.sequence(children.map { x =>
      val node = new ZkNode[APIKey](x, keyNode)
      node.getData()
    }))
  }




}
