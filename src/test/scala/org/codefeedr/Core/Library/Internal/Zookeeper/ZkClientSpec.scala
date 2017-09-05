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

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.async.Async._

class ZkClientSpec  extends FlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {



  override def afterEach(): Unit = {
    Await.ready(ZkClient().DeleteRecursive("/"), Duration(10, SECONDS))
  }

  "A ZkClient" should "Be able to create nodes "in async {
    await(ZkClient().Create("/test/somenode"))
    assert(await(ZkClient().Exists("/test/somenode")))
  }

  "A ZkClient" should "Be able to delete nodes "in async {
    await(ZkClient().Create("/test/somenode"))
    assert(await(ZkClient().Exists("/test/somenode")))
    await(ZkClient().Delete("/test/somenode"))
    assert(!await(ZkClient().Exists("/test/somenode")))
  }

  "A ZkClient" should "Be able to delete nodes recursively "in async {
    await(ZkClient().Create("/test/somenode"))
    assert(await(ZkClient().Exists("/test/somenode")))
    await(ZkClient().DeleteRecursive("/test"))
    assert(!await(ZkClient().Exists("/test/somenode")))
  }

  "A ZkClient" should "Be able to create nodes with data "in async {
    await(ZkClient().CreateWithData("/test/somenode", "hello"))
    assert(await(ZkClient().Exists("/test/somenode")) )
    assert(await(ZkClient().GetData[String]("\"/test/somenode\"")) == "hello")
  }

  "A ZkClient" should "Be able to create nodes without data and set data"in async {
    await(ZkClient().Create("/test/somenode"))
    assert(await(ZkClient().Exists("/test/somenode")) )
    assert(await(ZkClient().GetData[String]("\"/test/somenode\"")) == null)
    await(ZkClient().SetData("/test/somenode", "test"))
    assert(await(ZkClient().GetData[String]("\"/test/somenode\"")) == "test")
  }

  "A ZkClient" should "Be able to await construction of a child" in async {
    await(ZkClient().Create("/test"))
    val future = ZkClient().AwaitChild("/test","child").map(_ => assert(true))
    assertThrows[TimeoutException](Await.ready(future, Duration(100, MILLISECONDS)))
    await(ZkClient().Create("/test/child"))
    Await.ready(future, Duration(100, MILLISECONDS))
  }

  "A ZkClient" should "Be able to await removal of a node" in async {
    await(ZkClient().Create("/test/somenode"))
    val future = ZkClient().AwaitRemoval("/test/somenode").map(_ => assert(true))
    assertThrows[TimeoutException](Await.ready(future, Duration(100, MILLISECONDS)))
    await(ZkClient().Delete("/test/somenode"))
    Await.ready(future, Duration(100, MILLISECONDS))
  }


  "A ZkClient" should "Be able to await a condition based on a given method" in async {
    await(ZkClient().CreateWithData("/test/somenode", "nothello"))
    val future = ZkClient().AwaitCondition("/test/somenode", (o:String) => o == "hello").map(_ => assert(true))
    assertThrows[TimeoutException](Await.ready(future, Duration(100, MILLISECONDS)))
    await(ZkClient().SetData("/test/somenode", "hello"))
    Await.ready(future, Duration(100, MILLISECONDS))
  }



}
