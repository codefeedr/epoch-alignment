/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.codefeedr.Engine.Query

import java.util.concurrent.Executors

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.codefeedr.Library.TestCollector.logger
import org.codefeedr.Library.{CollectionPlugin, MyOwnIntegerObject, SimplePlugin}
import org.codefeedr.Model.TrailedRecord
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, Matchers}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import org.apache.flink.streaming.api.scala._

case class TestJoinObject(id:Long, group:Long, message:String)
case class TestJoinGroup(id:Long, name: String)


import scala.async.Async.{async, await}


object TestCollector extends LazyLogging {
  var collectedData: mutable.MutableList[TrailedRecord] =
    mutable.MutableList[TrailedRecord]()

  def collect(item: TrailedRecord): Unit = {
    logger.debug(s"${item.record.data(1).asInstanceOf[String]}-${item.record.data(2).asInstanceOf[String]} recieved")
    this.synchronized {
      collectedData += item
    }
  }
}


/**
  * Integration test for a join
  * Created by Niels on 04/08/2017.
  */
class JoinQuerySpec extends AsyncFlatSpec with Matchers with BeforeAndAfterAll with LazyLogging{

  implicit override def executionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutorService(Executors.newWorkStealingPool(16))

  "An innerjoin Query" should " produce a record for each join candidate" in {

    val objects = Array(
      TestJoinObject(1,1,"Message 1"),
      TestJoinObject(2,1,"Message 2"),
      TestJoinObject(3,1,"Message 3")
    )
    val groups = Array(TestJoinGroup(1,"Group 2"))
    async {
      async {
        val env1 = StreamExecutionEnvironment.createLocalEnvironment()
        env1.setParallelism(2)
        logger.debug("Composing env1")
        await(new CollectionPlugin(objects).Compose(env1))
        logger.debug("Starting env1")
        env1.execute()
        logger.debug("env1 completed")
      }
      async {
        val env2 = StreamExecutionEnvironment.createLocalEnvironment()
        env2.setParallelism(2)
        logger.debug("Composing env2")
        await(new CollectionPlugin(groups).Compose(env2))
        logger.debug("Starting env2")
        env2.execute()
        logger.debug("env2 completed")
      }

      async {
        val query = Join(SubjectSource("TestJoinObject"), SubjectSource("TestJoinGroup"), List("group"), List("id"), List("id", "message"), List("name"), "groupedMessage")
        val queryEnv = StreamExecutionEnvironment.createLocalEnvironment()
        queryEnv.setParallelism(2)
        logger.debug("Creating query Composer")
        val composer = await(StreamComposerFactory.GetComposer(query))
        logger.debug("Composing queryEnv")
        val resultStream = composer.Compose(queryEnv)
        resultStream.addSink(data => TestCollector.collect(data))
        logger.debug("Starting queryEnv")
        queryEnv.execute()
        logger.debug("queryenv completed")
      }
    }
    Thread.sleep(20000)


    assert(TestCollector.collectedData.size == 3)
  }


}
