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
  
  override def beforeEach() : Unit = {
    Await.ready(keyNode.deleteRecursive(), Duration(1, SECONDS))
  }

  "The APIKeyManager" should "correctly store all keys in ZooKeeper" in async {
    val keyManager = new APIKeyManager()

    await(keyManager.saveToZK())
    val children = await(zkClient.GetChildren("/keys")).toList

    assert(children.size == keyManager.loadKeys().size)
    assert(children == keyManager.loadKeys().map(_.key).reverse)
  }

  "The APIKeyManager" should "lock a key when a key is requested" in async {
    val keyManager = new APIKeyManager()
    await(keyManager.saveToZK())

    val keyLocked = await(keyManager.acquireKey())

    val children = await(zkClient.GetChildren("/keys")).toList
    val childrenData = await(Future.sequence(children.map { x =>
      val node = new ZkNode[APIKey](x, keyNode)
      node.getData()
    }))

    val foundKey = childrenData.find(x => x.get.key == keyLocked.get.key).get
    assert(foundKey.get.available == false) //foundKey should be locked
  }

  "The APIKeyManager" should "lock multiple keys when multiple requests are done" in async {
    val keyManager = new APIKeyManager()
    await(keyManager.saveToZK())

    val calls = keyManager.acquireKey() :: keyManager.acquireKey() :: keyManager.acquireKey() :: Nil
    val keysLocked = await(Future.sequence(calls))

    val children = await(zkClient.GetChildren("/keys")).toList
    val childrenData = await(Future.sequence(children.map { x =>
      val node = new ZkNode[APIKey](x, keyNode)
      node.getData()
    }))

    assert(childrenData.filter(!_.get.available).size == 2) //2 keys are locked
    assert(keysLocked.filter(_.isEmpty).size == 1) //1 getKey called returned None
    assert(keysLocked.filter(!_.isEmpty).size == 2) //2 getKeys called returned an actual key
  }

  "The APIKeyManager" should "unlock a key after it has been released" in async {
    val keyManager = new APIKeyManager()
    await(keyManager.saveToZK())

    val keyLocked = await(keyManager.acquireKey()).get

    //release the key again
    keyManager.updateAndReleaseKey(keyLocked.copy(requestsLeft = 0))

    val children = await(zkClient.GetChildren("/keys")).toList
    val childrenData = await(Future.sequence(children.map { x =>
      val node = new ZkNode[APIKey](x, keyNode)
      node.getData()
    }))

    val foundKey = childrenData.find(x => x.get.key == keyLocked.key).get
    assert(foundKey.get.available == true) //foundKey should be available again
    assert(foundKey.get.requestsLeft == 0) //requests left should be equal to zero
  }




}
