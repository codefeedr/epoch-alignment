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

import org.codefeedr.core.library.metastore.MetaRootNode
import org.codefeedr.core.LibraryServiceSpec
import org.scalatest._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.async.Async._
import org.scalatest.tagobjects.Slow
import org.codefeedr.util.futureExtensions._

class ZkClientSpec  extends LibraryServiceSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  override def afterEach(): Unit = {
    super.afterEach()
    Await.ready(libraryServices.zkClient.deleteRecursive("/"), Duration(1, SECONDS))
  }

  "libraryServices.zkClient.Create()" should "Be able to create nodes "in async {
    assert(!await(libraryServices.zkClient.exists("/ZkClientSpec/somenode")))
    await(libraryServices.zkClient.create("/ZkClientSpec/somenode"))
    assert(await(libraryServices.zkClient.exists("/ZkClientSpec/somenode")))
  }

  "libraryServices.zkClient.Delete()" should "Be able to delete nodes "in async {
    assert(!await(libraryServices.zkClient.exists("/ZkClientSpec/somenode")))
    await(libraryServices.zkClient.create("/ZkClientSpec/somenode"))
    assert(await(libraryServices.zkClient.exists("/ZkClientSpec/somenode")))
    await(libraryServices.zkClient.Delete("/ZkClientSpec/somenode"))
    assert(!await(libraryServices.zkClient.exists("/ZkClientSpec/somenode")))
  }

  "libraryServices.zkClient.DeleteRecursive()" should "Be able to delete nodes recursively "in async {
    await(libraryServices.zkClient.create("/ZkClientSpec/somenode"))
    assert(await(libraryServices.zkClient.exists("/ZkClientSpec/somenode")))
    await(libraryServices.zkClient.deleteRecursive("/ZkClientSpec"))
    assert(!await(libraryServices.zkClient.exists("/ZkClientSpec/somenode")))
  }

  "libraryServices.zkClient.CreateWithDate()" should "Be able to create nodes with data "in async {
    await(libraryServices.zkClient.createWithData("/ZkClientSpec/somenode", "hello"))
    assert(await(libraryServices.zkClient.exists("/ZkClientSpec/somenode")) )
    assert(await(libraryServices.zkClient.getData[String]("/ZkClientSpec/somenode")).get == "hello")
  }

  "libraryServices.zkClient.SetData()" should "Be able to create nodes without data and set data"in async {
    await(libraryServices.zkClient.create("/ZkClientSpec/somenode"))
    assert(await(libraryServices.zkClient.exists("/ZkClientSpec/somenode")))
    assert(await(libraryServices.zkClient.getData[String]("/ZkClientSpec/somenode")).isEmpty)
    await(libraryServices.zkClient.setData("/ZkClientSpec/somenode", "test"))
    assert(await(libraryServices.zkClient.getData[String]("/ZkClientSpec/somenode")).get =="test")
  }

  "libraryServices.zkClient.GetChildren()" should "Be able to retrieve children of a node" in async {
    await(libraryServices.zkClient.create("/ZkClientSpec/node1"))
    await(libraryServices.zkClient.create("/ZkClientSpec/node2"))
    val children = await(libraryServices.zkClient.GetChildren("/ZkClientSpec"))
    assert(children.size == 2)
    assert(children.exists(o => o == "node1"))
    assert(children.exists(o => o== "node2"))
  }



  "libraryServices.zkClient.AwaitChild()" should "Be able to await construction of a child" taggedAs Slow in async {
    await(libraryServices.zkClient.create("/ZkClientSpec"))
    val future = libraryServices.zkClient.awaitChild("/ZkClientSpec","child").map(_ => assert(true))
    await(future.assertTimeout())
    await(libraryServices.zkClient.create("/ZkClientSpec/child"))
    Await.ready(future, Duration(1, SECONDS))
    assert(true)
  }

  "libraryServices.zkClient.AwaitRemoval()" should "Be able to await removal of a node" taggedAs Slow in async {
    await(libraryServices.zkClient.create("/ZkClientSpec/somenode"))
    val future = libraryServices.zkClient.awaitRemoval("/ZkClientSpec/somenode").map(_ => assert(true))
    await(future.assertTimeout())
    libraryServices.zkClient.Delete("/ZkClientSpec/somenode")
    await(future)
  }


  "libraryServices.zkClient.AwaitCondition()" should "Be able to await a condition based on a given method" taggedAs Slow in async {
    await(libraryServices.zkClient.createWithData("/ZkClientSpec/somenode", "nothello"))
    val future = libraryServices.zkClient.awaitCondition("/ZkClientSpec/somenode", (o:String) => o == "hello").map(_ => assert(true))
    await(future.assertTimeout())
    await(libraryServices.zkClient.setData("/ZkClientSpec/somenode", "hello"))
    Await.ready(future, Duration(1, SECONDS))
    assert(true)
  }
}
