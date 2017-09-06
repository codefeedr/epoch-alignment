

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

package org.codefeedr.Core.Library

import java.util.concurrent.Executors

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, BeforeAndAfterEach, Matchers}
import org.apache.flink.streaming.api.scala._
import org.codefeedr.Core.KafkaTest
import org.codefeedr.Core.Library.Internal.Zookeeper.ZkClient
import org.codefeedr.Model.TrailedRecord
import org.scalatest.tagobjects.Slow

import scala.collection.mutable
import scala.concurrent.{TimeoutException, _}
import scala.concurrent.duration._
import scala.async.Async.{async, await}

@SerialVersionUID(100L)
case class MyOwnIntegerObject(value: Int) extends Serializable

object TestCollector extends LazyLogging {
  var collectedData: mutable.MutableList[(Int, MyOwnIntegerObject)] =
    mutable.MutableList[(Int, MyOwnIntegerObject)]()

  def collect(item: (Int, MyOwnIntegerObject)): Unit = {
    logger.debug(s"${item._1} recieved ${item._2}")
    this.synchronized {
      collectedData += item
    }
  }
}

/**
  * This is more of an integration test than unit test
  * Created by Niels on 14/07/2017.
  */
class KafkaSubjectSpec extends AsyncFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with LazyLogging {
  this: LibraryServices =>

  //These tests must run in parallel
  implicit override def executionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutorService(Executors.newWorkStealingPool(16))

  val parallelism = 2
  val testSubjectName = "MyOwnIntegerObject"

  override def beforeEach(): Unit = {
    Await.ready(zkClient.DeleteRecursive("/"), Duration(1, SECONDS))
    Await.ready(subjectLibrary.Initialize(),Duration(1, SECONDS))
    TestCollector.collectedData = mutable.MutableList[(Int, MyOwnIntegerObject)]()
  }


  /**
    * Creates test input
    * @return
    */
  def CreateTestInput():Future[Unit] = async {
    //Create a sink function
    val sink = await(SubjectFactory.GetSink[MyOwnIntegerObject])

    //Source environment
    val env = StreamExecutionEnvironment.createLocalEnvironment()
    env.setParallelism(parallelism)
    env.fromCollection(mutable.Set(1, 2, 3).toSeq).map(o => MyOwnIntegerObject(o)).addSink(sink)
    logger.debug("Starting test sequence")
    env.execute()
    logger.debug("Finished producing test sequence")
  }

  def CreateSourceQuery(nr: Int):Future[Unit] = {
    Future {
      new MyOwnSourceQuery(nr).run()
    }
  }


  "Kafka-Sinks" should "retrieve all messages published by a source" taggedAs (Slow, KafkaTest) in async {
    //Create persistent environment so that the finite source will not immediately close the type
    val t = await(subjectLibrary.GetOrCreateType[MyOwnIntegerObject](persistent = true))

    //Creating fake query environments
    val environments = Future.sequence(Seq(CreateSourceQuery(1),CreateSourceQuery(2) ,CreateSourceQuery(3)))

    //Generate some test input
    await(CreateTestInput())

    Console.println("Closing subject type, should close the queries")
    await(subjectLibrary.Close(testSubjectName))
    Console.println("Waiting for completion of queries")
    await(environments)
    println("Completed")

    //Clean up subject
    await(subjectLibrary.UnRegisterSubject(testSubjectName))

    //Assert results
    assert(TestCollector.collectedData.count(o => o._1 == 1) == 3)
    assert(TestCollector.collectedData.count(o => o._1 == 2) == 3)
    assert(TestCollector.collectedData.count(o => o._1 == 3) == 3)
    assert(TestCollector.collectedData.count(o => o._2.value == 1) == 3)
    assert(TestCollector.collectedData.count(o => o._2.value == 2) == 3)
    assert(TestCollector.collectedData.count(o => o._2.value == 3) == 3)
  }


  it should " still receive data if they are created before the sink" taggedAs (Slow, KafkaTest) in async {
    //No persistent type needed now because the sources are created first
    val t = await(subjectLibrary.GetOrCreateType[MyOwnIntegerObject](persistent = true))

    await(CreateTestInput())

    //Creating fake query environments
    val environments = Future.sequence(Seq(CreateSourceQuery(1),CreateSourceQuery(2) ,CreateSourceQuery(3)))


    Console.println("Closing subject type, should close the queries")
    await(subjectLibrary.Close(testSubjectName))
    Console.println("Waiting for completion of queries")
    await(environments)
    println("Completed")

    assert(TestCollector.collectedData.count(o => o._1 == 1) == 3)
    assert(TestCollector.collectedData.count(o => o._1 == 2) == 3)
    assert(TestCollector.collectedData.count(o => o._1 == 3) == 3)
    assert(TestCollector.collectedData.count(o => o._2.value == 1) == 3)
    assert(TestCollector.collectedData.count(o => o._2.value == 2) == 3)
    assert(TestCollector.collectedData.count(o => o._2.value == 3) == 3)
  }

  it should " be able to recieve data from multiple sinks" taggedAs (Slow, KafkaTest) in async {
    await(subjectLibrary.GetOrCreateType[MyOwnIntegerObject](persistent = true))

    val environments = Future.sequence(Seq(CreateSourceQuery(1),CreateSourceQuery(2) ,CreateSourceQuery(3)))

    await(Future.sequence(for (_ <- 1 to 3) yield {
      CreateTestInput()
    }))

    Console.println("Closing subject type, should close the queries")
    await(subjectLibrary.Close(testSubjectName))
    Console.println("Waiting for completion of queries")
    await(environments)
    println("Completed")

    assert(TestCollector.collectedData.count(o => o._1 == 1) == 9)
    assert(TestCollector.collectedData.count(o => o._1 == 2) == 9)
    assert(TestCollector.collectedData.count(o => o._1 == 3) == 9)
    assert(TestCollector.collectedData.count(o => o._2.value == 1) == 9)
    assert(TestCollector.collectedData.count(o => o._2.value == 2) == 9)
    assert(TestCollector.collectedData.count(o => o._2.value == 3) == 9)

  }


  class MyOwnSourceQuery(nr: Int) extends Runnable with LazyLogging {
    override def run(): Unit = {
      val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism)
      Await.ready(createTopology(env, nr), Duration(120, SECONDS))
      logger.debug(s"Starting environment $nr")
      env.execute(s"job$nr")
      logger.debug(s"Environment $nr finished")
    }

    /**
      * Create a simple topology that converts records back into MyOwnIntegerObjects and passes it to a testcollecter
      *
      * @param env Stream Execution Environment to create topology on
      * @param nr  Number used to identify topology in the test
      */

    def createTopology(env: StreamExecutionEnvironment, nr: Int): Future[Unit] =
    //Construct a new source using the subjectFactory
      subjectLibrary
        .GetOrCreateType[MyOwnIntegerObject]()
        .map(subjectType => {
          val transformer = SubjectFactory.GetUnTransformer[MyOwnIntegerObject](subjectType)
          val source = SubjectFactory.GetSource(subjectType)
          env
            .addSource(source)
            .map(transformer)
            .map(o => Tuple2(nr, o))
            .addSink(o => TestCollector.collect(o))
        })
  }
}
