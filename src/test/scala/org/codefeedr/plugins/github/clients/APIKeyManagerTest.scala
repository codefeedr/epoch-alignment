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

  //first key has 5000 requests
  //second key has 3000 requests
  val keyNode = new KeysNode
  val keyManager = new APIKeyManager()

  override def beforeEach() : Unit = {
    Await.ready(keyNode.deleteRecursive(), Duration(1, SECONDS))
    Await.result(keyManager.init((x) => x), Duration(1, SECONDS))
  }
  override def afterEach() : Unit = {
    Await.ready(keyNode.deleteRecursive(), Duration(1, SECONDS))
  }

  "The APIKeyManager" should "correctly store all keys in ZooKeeper" in async {
    val children = await(zkClient.GetChildren("/keys")).toList

    assert(children.size == 2) //2 children
  }

  "The APIKeyManager" should "lock a key when a key is requested" in async {

    val keyLocked = await(keyManager.acquireKey())

    val childrenData = await(getChildrenData())

    val foundKey = childrenData.find(x => x.get.key == keyLocked.get.key).get
    assert(foundKey.get.requestsLeft == 4999) //foundKey should be decremented
  }

  "The APIKeyManager" should "lock multiple keys when multiple requests are done" in async {
    val calls = keyManager.acquireKey() :: keyManager.acquireKey() :: keyManager.acquireKey(2) :: Nil
    val keysLocked = await(Future.sequence(calls))

    val childrenData = await(getChildrenData())

    assert(childrenData.filter(x => x.get.requestsLeft == 4996) == 1) //decremented by 4
    assert(keysLocked.filter(!_.isEmpty).size == 3) //2 getKeys called returned an actual key
  }


  "The APIKeyManager" should "resets a key after the reset time (synchronous requests)" in async {
    val firstKey = await(keyManager.acquireKey(5000)) //this one should be reset afterwards because resetTime is 0
    val secondKey = await(keyManager.acquireKey(3000)) //firstKey should now be reset and secondKey is 0

    val childrenData = await(getChildrenData())
    val firstKeyFound = childrenData.find(x => x.get.key == firstKey.get.key).get
    val secondKeyFound = childrenData.find(x => x.get.key == secondKey.get.key).get

    //first key is reset
    assert(firstKeyFound.get.requestsLeft == firstKeyFound.get.requestLimit)
    assert(firstKeyFound.get.resetTime == 0) //reset time is also set to 0

    //second key has to be reset
    assert(secondKeyFound.get.requestsLeft == 0)
  }

  "The APIKeyManager" should "resets a key after the reset time (concurrent requests) " in async {
    val firstKey = keyManager.acquireKey(5000) //both requests are done at the same time
    val secondKey = keyManager.acquireKey(3000)

    val keys = await(Future.sequence(firstKey :: secondKey :: Nil))

    val childrenData = await(getChildrenData())

    val firstKeyFound = childrenData.find(x => x.get.key == keys(0).get.key).get
    val secondKeyFound = childrenData.find(x => x.get.key == keys(1).get.key).get

    //both keys are not reset
    assert(firstKeyFound.get.requestsLeft == 0)
    assert(secondKeyFound.get.requestsLeft == 0)

    //if we do a third request (with no decrement), both keys should be resetted
    val thirdRequest = await(keyManager.acquireKey(0))

    val childrenDataUpdated = await(getChildrenData())

    //get third key
    val thirdKeyFound = childrenDataUpdated.find(x => x.get.key == thirdRequest.get.key).get

    assert(childrenDataUpdated.filter(x => x.get.requestsLeft > 0).size == 2) //2 keys with > 0 requests left
    assert(thirdKeyFound.get.requestsLeft == 10)
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
