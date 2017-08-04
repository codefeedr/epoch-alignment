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
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.codefeedr.Library.{CollectionPlugin, SubjectLibrary}
import org.codefeedr.Model.TrailedRecord
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterEach, Matchers}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

case class TestJoinObject(id:Long, group:Long, message:String)
case class TestJoinGroup(id:Long, name: String)


import scala.async.Async.{async, await}


object TestCollector extends LazyLogging {

  def reset(): Unit = {
    collectedData = mutable.MutableList[TrailedRecord]()
  }

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
class JoinQuerySpec extends AsyncFlatSpec with Matchers with BeforeAndAfterEach with LazyLogging{
  var counter:Int = 0

  implicit override def executionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutorService(Executors.newWorkStealingPool(16))

  override def beforeEach() {
    TestCollector.reset()
  }


  /**
    * Utility function for tests that creates a source environment with the given data
    * @param data the data to create environment for
    * @tparam T type of the data
    * @return A future that returns when all data has been pushed to kakfa
    */
  def CreateSourceEnvironment[T:ru.TypeTag: ClassTag:TypeInformation](data: Array[T]):Future[Unit] = async {
    val nr = counter
    counter+=1
    val env = StreamExecutionEnvironment.createLocalEnvironment()
    env.setParallelism(1)
    logger.debug(s"Composing env$nr")
    await(new CollectionPlugin(data).Compose(env))
    logger.debug(s"Starting env$nr")
    env.execute()
    logger.debug(s"env$nr completed")
  }

  /**
    * Utility function that creates a query environment, and probably never completes
    * @param query The query environment
    * @return When the environment is done, probably never
    */
  def CreateQueryEnvironment(query: QueryTree):Future[Unit] = async {
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


  "An innerjoin Query" should " produce a record for each join candidate" in {
    val objects = Array(
      TestJoinObject(1, 1, "Message 1"),
      TestJoinObject(2, 1, "Message 2"),
      TestJoinObject(3, 1, "Message 3")
    )

    val groups = Array(TestJoinGroup(1, "Group 1"))
    val query = Join(SubjectSource("TestJoinObject"), SubjectSource("TestJoinGroup"), Array("group"), Array("id"), Array("id", "message"), Array("name"), "groupedMessage")

    async {
      val queryEnvJob = CreateQueryEnvironment(query)
      //Lift the exception so you actually see it
      queryEnvJob.onFailure {
        case t: Throwable => throw t
      }
      //Add sources and wait for them to finish
      await(CreateSourceEnvironment(objects))
      await(CreateSourceEnvironment(groups))
      await(SubjectLibrary.UnRegisterSubject("TestJoinObject"))
      await(SubjectLibrary.UnRegisterSubject("TestJoinGroup"))
      assert(TestCollector.collectedData.size == 3)
    }
  }


  "An innerjoin Query" should " produce no records if no join candidates are found" in {
    val objects = Array(
      TestJoinObject(1, 1, "Message 1"),
      TestJoinObject(2, 1, "Message 2"),
      TestJoinObject(3, 1, "Message 3")
    )

    val groups = Array(TestJoinGroup(2, "Group 2"),TestJoinGroup(3, "Group 3"))
    val query = Join(SubjectSource("TestJoinObject"), SubjectSource("TestJoinGroup"), Array("group"), Array("id"), Array("id", "message"), Array("name"), "groupedMessage")

    async {
      val queryEnvJob = CreateQueryEnvironment(query)
      //Lift the exception so you actually see it
      queryEnvJob.onFailure {
        case t: Throwable => throw t
      }
      //Add sources and wait for them to finish
      await(CreateSourceEnvironment(objects))
      await(CreateSourceEnvironment(groups))
      await(SubjectLibrary.UnRegisterSubject("TestJoinObject"))
      await(SubjectLibrary.UnRegisterSubject("TestJoinGroup"))
      assert(TestCollector.collectedData.isEmpty)
    }
  }


  "An innerjoin Query" should " Only produce events for new combinations" in {
    val objects = Array(
      TestJoinObject(1, 1, "Message 1"),
      TestJoinObject(2, 1, "Message 2"),
      TestJoinObject(3, 1, "Message 3")
    )

    val groups = Array(TestJoinGroup(1, "Group 1"),TestJoinGroup(1, "Group 1 duplicate 1"),TestJoinGroup(1, "Group 1 duplicate 2"))
    val query = Join(SubjectSource("TestJoinObject"), SubjectSource("TestJoinGroup"), Array("group"), Array("id"), Array("id", "message"), Array("name"), "groupedMessage")

    async {
      val queryEnvJob = CreateQueryEnvironment(query)
      //Lift the exception so you actually see it
      queryEnvJob.onFailure {
        case t: Throwable => throw t
      }
      //Add sources and wait for them to finish
      await(CreateSourceEnvironment(objects))
      await(CreateSourceEnvironment(groups))
      await(SubjectLibrary.UnRegisterSubject("TestJoinObject"))
      await(SubjectLibrary.UnRegisterSubject("TestJoinGroup"))
      assert(TestCollector.collectedData.size == 9)
    }
  }

}
