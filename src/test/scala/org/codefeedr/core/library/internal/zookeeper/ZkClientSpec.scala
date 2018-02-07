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

import org.codefeedr.core.library.LibraryServices
import org.codefeedr.core.library.metastore.MetaRootNode
import org.codefeedr.core.LibraryServiceSpec
import org.scalatest._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.async.Async._
import org.scalatest.tagobjects.Slow
import org.codefeedr.util.FutureExtensions._

class ZkClientSpec  extends LibraryServiceSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  /**
    * After each test, make sure to clean the zookeeper store
    */
  override def beforeEach(): Unit = {
    Await.ready(zkClient.DeleteRecursive("/"), Duration(1, SECONDS))
  }

  override def afterEach(): Unit = {
    Await.ready(zkClient.DeleteRecursive("/"), Duration(1, SECONDS))
  }

  "ZkClient.Create()" should "Be able to create nodes "in async {
    assert(!await(zkClient.Exists("/ZkClientSpec/somenode")))
    await(zkClient.Create("/ZkClientSpec/somenode"))
    assert(await(zkClient.Exists("/ZkClientSpec/somenode")))
  }

  "ZkClient.Delete()" should "Be able to delete nodes "in async {
    assert(!await(zkClient.Exists("/ZkClientSpec/somenode")))
    await(zkClient.Create("/ZkClientSpec/somenode"))
    assert(await(zkClient.Exists("/ZkClientSpec/somenode")))
    await(zkClient.Delete("/ZkClientSpec/somenode"))
    assert(!await(zkClient.Exists("/ZkClientSpec/somenode")))
  }

  "ZkClient.DeleteRecursive()" should "Be able to delete nodes recursively "in async {
    await(zkClient.Create("/ZkClientSpec/somenode"))
    assert(await(zkClient.Exists("/ZkClientSpec/somenode")))
    await(zkClient.DeleteRecursive("/ZkClientSpec"))
    assert(!await(zkClient.Exists("/ZkClientSpec/somenode")))
  }

  "ZkClient.CreateWithDate()" should "Be able to create nodes with data "in async {
    await(zkClient.CreateWithData("/ZkClientSpec/somenode", "hello"))
    assert(await(zkClient.Exists("/ZkClientSpec/somenode")) )
    assert(await(zkClient.GetData[String]("/ZkClientSpec/somenode")).get == "hello")
  }

  "ZkClient.SetData()" should "Be able to create nodes without data and set data"in async {
    await(zkClient.Create("/ZkClientSpec/somenode"))
    assert(await(zkClient.Exists("/ZkClientSpec/somenode")))
    assert(await(zkClient.GetData[String]("/ZkClientSpec/somenode")).isEmpty)
    await(zkClient.SetData("/ZkClientSpec/somenode", "test"))
    assert(await(zkClient.GetData[String]("/ZkClientSpec/somenode")).get =="test")
  }

  "ZkClient.GetChildren()" should "Be able to retrieve children of a node" in async {
    await(zkClient.Create("/ZkClientSpec/node1"))
    await(zkClient.Create("/ZkClientSpec/node2"))
    val children = await(zkClient.GetChildren("/ZkClientSpec"))
    assert(children.size == 2)
    assert(children.exists(o => o == "node1"))
    assert(children.exists(o => o== "node2"))
  }



  "ZkClient.AwaitChild()" should "Be able to await construction of a child" taggedAs Slow in async {
    await(zkClient.Create("/ZkClientSpec"))
    val future = zkClient.AwaitChild("/ZkClientSpec","child").map(_ => assert(true))
    await(future.AssertTimeout())
    await(zkClient.Create("/ZkClientSpec/child"))
    Await.ready(future, Duration(1, SECONDS))
    assert(true)
  }

  "ZkClient.AwaitRemoval()" should "Be able to await removal of a node" taggedAs Slow in async {
    await(zkClient.Create("/ZkClientSpec/somenode"))
    val future = zkClient.AwaitRemoval("/ZkClientSpec/somenode").map(_ => assert(true))
    await(future.AssertTimeout())
    zkClient.Delete("/ZkClientSpec/somenode")
    await(future)
  }


  "ZkClient.AwaitCondition()" should "Be able to await a condition based on a given method" taggedAs Slow in async {
    await(zkClient.CreateWithData("/ZkClientSpec/somenode", "nothello"))
    val future = zkClient.AwaitCondition("/ZkClientSpec/somenode", (o:String) => o == "hello").map(_ => assert(true))
    await(future.AssertTimeout())
    await(zkClient.SetData("/ZkClientSpec/somenode", "hello"))
    Await.ready(future, Duration(1, SECONDS))
    assert(true)
  }
}
