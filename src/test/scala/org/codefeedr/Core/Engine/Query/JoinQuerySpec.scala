

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

package org.codefeedr.Core.Engine.Query


import org.apache.flink.api.scala._
import org.codefeedr.Core.{FullIntegrationSpec, KafkaTest}
import org.scalatest.tagobjects.Slow


case class TestJoinObject(id: Long, group: Long, message: String)
case class TestJoinGroup(id: Long, name: String)

import scala.async.Async.{async, await}


/**
  * Integration test for a join
  * Created by Niels on 04/08/2017.
  */
class JoinQuerySpec extends FullIntegrationSpec {

  "An InnerJoinQuery" should " produce a record for each join candidate" taggedAs (Slow, KafkaTest) in {
      val objects = Array(
        TestJoinObject(1, 1, "Message 1"),
        TestJoinObject(2, 1, "Message 2"),
        TestJoinObject(3, 1, "Message 3")
      )

      val groups = Array(TestJoinGroup(1, "Group 1"))
      val query = Join(SubjectSource("TestJoinObject"),
        SubjectSource("TestJoinGroup"),
        Array("group"),
        Array("id"),
        Array("id", "message"),
        Array("name"),
        "groupedMessage")

    async {
      val objectType = await(RunSourceEnvironment(objects))
      val groupType = await(RunSourceEnvironment(groups))

      assert(await(AwaitAllData(objectType)).size == 3)
      assert(await(AwaitAllData(groupType)).size == 1)

      val resultType = await(RunQueryEnvironment(query))
      val result = await(AwaitAllData(resultType))
      assert(result.size == 3)
    }
  }


  it should " produce no records if no join candidates are found" taggedAs (Slow, KafkaTest) in {
    val objects = Array(
      TestJoinObject(1, 1, "Message 1"),
      TestJoinObject(2, 1, "Message 2"),
      TestJoinObject(3, 1, "Message 3")
    )

    val groups = Array(TestJoinGroup(2, "Group 2"), TestJoinGroup(3, "Group 3"))

    val query = Join(SubjectSource("TestJoinObject"),
                     SubjectSource("TestJoinGroup"),
                     Array("group"),
                     Array("id"),
                     Array("id", "message"),
                     Array("name"),
                     "groupedMessage")

    async {
      //Add sources and wait for them to finish
      val objectType = await(RunSourceEnvironment(objects))
      val groupType = await(RunSourceEnvironment(groups))

      assert(await(AwaitAllData(objectType)).size == 3)
      assert(await(AwaitAllData(groupType)).size == 1)

      val resultType = await(RunQueryEnvironment(query))
      val result = await(AwaitAllData(resultType))
      assert(result.isEmpty)
    }
  }

  it should " Only produce events for new combinations" taggedAs (Slow, KafkaTest) in {
    //Create a set of objects
    val objects = Array(
      TestJoinObject(1, 1, "Message 1"),
      TestJoinObject(2, 1, "Message 2"),
      TestJoinObject(3, 1, "Message 3")
    )

    //Create a set of groups to join with
    val groups = Array(TestJoinGroup(1, "Group 1"),
                       TestJoinGroup(1, "Group 1 duplicate 1"),
                       TestJoinGroup(1, "Group 1 duplicate 2"))

    //Create the query
    val query = Join(SubjectSource("TestJoinObject"),
                     SubjectSource("TestJoinGroup"),
                     Array("group"),
                     Array("id"),
                     Array("id", "message"),
                     Array("name"),
                     "groupedMessage")

    //Run all environments
    async {
      //Add sources and wait for them to finish
      val objectType = await(RunSourceEnvironment(objects))
      val groupType = await(RunSourceEnvironment(groups))

      assert(await(AwaitAllData(objectType)).size == 3)
      assert(await(AwaitAllData(groupType)).size == 3)

      val queryResultType = await(RunQueryEnvironment(query))
      val result = await(AwaitAllData(queryResultType))
      assert(result.size == 9)
    }
  }
}
