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

import org.scalatest._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.async.Async._
import org.scalatest.tagobjects.Slow

class ZkClientSpec  extends AsyncFlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {



  override def beforeEach(): Unit = {
    Await.ready(ZkClient().DeleteRecursive("/"), Duration(1, SECONDS))
  }

  "A ZkClient" should "Be able to create nodes "in async {
    assert(!await(ZkClient().Exists("/ZkClientSpec/somenode")))
    await(ZkClient().Create("/ZkClientSpec/somenode"))
    assert(await(ZkClient().Exists("/ZkClientSpec/somenode")))
  }

  "A ZkClient" should "Be able to delete nodes "in async {
    assert(!await(ZkClient().Exists("/ZkClientSpec/somenode")))
    await(ZkClient().Create("/ZkClientSpec/somenode"))
    assert(await(ZkClient().Exists("/ZkClientSpec/somenode")))
    await(ZkClient().Delete("/ZkClientSpec/somenode"))
    assert(!await(ZkClient().Exists("/ZkClientSpec/somenode")))
  }

  "A ZkClient" should "Be able to delete nodes recursively "in async {
    await(ZkClient().Create("/ZkClientSpec/somenode"))
    assert(await(ZkClient().Exists("/ZkClientSpec/somenode")))
    await(ZkClient().DeleteRecursive("/ZkClientSpec"))
    assert(!await(ZkClient().Exists("/ZkClientSpec/somenode")))
  }

  "A ZkClient" should "Be able to create nodes with data "in async {
    await(ZkClient().CreateWithData("/ZkClientSpec/somenode", "hello"))
    assert(await(ZkClient().Exists("/ZkClientSpec/somenode")) )
    assert(await(ZkClient().GetData[String]("/ZkClientSpec/somenode")).get == "hello")
  }

  "A ZkClient" should "Be able to create nodes without data and set data"in async {
    await(ZkClient().Create("/ZkClientSpec/somenode"))
    assert(await(ZkClient().Exists("/ZkClientSpec/somenode")))
    assert(await(ZkClient().GetData[String]("/ZkClientSpec/somenode")).isEmpty)
    await(ZkClient().SetData("/ZkClientSpec/somenode", "test"))
    assert(await(ZkClient().GetData[String]("/ZkClientSpec/somenode")).get =="test")
  }

  "A ZkClient" should "Be able to retrieve children of a node" in async {
    await(ZkClient().Create("/ZkClientSpec/node1"))
    await(ZkClient().Create("/ZkClientSpec/node2"))
    val children = await(ZkClient().GetChildren("/ZkClientSpec"))
    assert(children.size == 2)
    assert(children.exists(o => o == "node1"))
    assert(children.exists(o => o== "node2"))
  }



  "A ZkClient" should "Be able to await construction of a child" taggedAs Slow in async {
    await(ZkClient().Create("/ZkClientSpec"))
    val future = ZkClient().AwaitChild("/ZkClientSpec","child").map(_ => assert(true))
    assertThrows[TimeoutException](Await.ready(future, Duration(100, MILLISECONDS)))
    await(ZkClient().Create("/ZkClientSpec/child"))
    Await.ready(future, Duration(1, SECONDS))
    assert(true)
  }

  "A ZkClient" should "Be able to await removal of a node" taggedAs Slow in async {
    await(ZkClient().Create("/ZkClientSpec/somenode"))
    val future = ZkClient().AwaitRemoval("/ZkClientSpec/somenode").map(_ => assert(true))
    assertThrows[TimeoutException](Await.ready(future, Duration(100, MILLISECONDS)))
    await(ZkClient().Delete("/ZkClientSpec/somenode"))
    Await.ready(future, Duration(1, SECONDS))
    assert(true)
  }


  "A ZkClient" should "Be able to await a condition based on a given method" taggedAs Slow in async {
    await(ZkClient().CreateWithData("/ZkClientSpec/somenode", "nothello"))
    val future = ZkClient().AwaitCondition("/ZkClientSpec/somenode", (o:String) => o == "hello").map(_ => assert(true))
    assertThrows[TimeoutException](Await.ready(future, Duration(100, MILLISECONDS)))
    await(ZkClient().SetData("/ZkClientSpec/somenode", "hello"))
    Await.ready(future, Duration(1, SECONDS))
    assert(true)
  }


  "A ZkNode" should "provide a simple api over ZkClient" in  async{
    val parent = ZkNode("/ZkClientSpec/some")
    val node = ZkNode("/ZkClientSpec/some/path")
    assert(!await(node.Exists()))
    await(parent.Create())
    assert(!await(node.Exists()))
    await(node.Create())
    assert(await(node.Exists()))
    assert(await(parent.GetChildren()).exists(o => o.Name == "path"))
  }

}
