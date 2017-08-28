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

package org.codefeedr.Core.Library

import java.util.concurrent.Executors

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, BeforeAndAfterEach, Matchers}
import org.apache.flink.streaming.api.scala._
import org.codefeedr.Core.KafkaTest
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
  //These tests must run in parallel
  implicit override def executionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutorService(Executors.newWorkStealingPool(16))

  val parallelism = 2
  val testSubjectName = "MyOwnIntegerObject"

  override def beforeEach(): Unit = {
    CleanSubject()
    TestCollector.collectedData = mutable.MutableList[(Int, MyOwnIntegerObject)]()
  }

  override def beforeAll(): Unit = {
    Await.ready(SubjectLibrary.Initialized, Duration.Inf)
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

  def CleanSubject(): Unit =  Await.ready(SubjectLibrary.ForceUnRegisterSubject(testSubjectName), Duration.Inf)

  def CreateSourceQuery(nr: Int):Future[Unit] = {
    Future {
      new MyOwnSourceQuery(nr).run()
    }
  }


  "Kafka-Sinks" should "retrieve all messages published by a source" taggedAs (Slow, KafkaTest) in async {
    //Create persistent environment so that the finite source will not immediately close the type
    await(SubjectLibrary.GetOrCreateType[MyOwnIntegerObject](persistent = true))

    //Generate some test input
    await(CreateTestInput())

    //Creating fake query environments
    val environments = Future.sequence(Seq(CreateSourceQuery(1),CreateSourceQuery(2) ,CreateSourceQuery(3)))

    Console.println("Closing subject type, should close the queries")
    await(SubjectLibrary.Close(testSubjectName))
    Console.println("Waiting for completion of queries")
    await(environments)
    println("Completed")

    //Clean up subject
    await(SubjectLibrary.UnRegisterSubject(testSubjectName))

    //Assert results
    assert(TestCollector.collectedData.count(o => o._1 == 1) == 3)
    assert(TestCollector.collectedData.count(o => o._1 == 2) == 3)
    assert(TestCollector.collectedData.count(o => o._1 == 3) == 3)
    assert(TestCollector.collectedData.count(o => o._2.value == 1) == 3)
    assert(TestCollector.collectedData.count(o => o._2.value == 2) == 3)
    assert(TestCollector.collectedData.count(o => o._2.value == 3) == 3)
  }

  "Kafka-Sinks" should " still receive data if they are created before the sink" taggedAs (Slow, KafkaTest) in async {
    //No persistent type needed now because the sources are created first
    await(SubjectLibrary.GetOrCreateType[MyOwnIntegerObject](persistent = false))

    //Creating fake query environments
    val environments = Future.sequence(Seq(CreateSourceQuery(1),CreateSourceQuery(2) ,CreateSourceQuery(3)))

    await(CreateTestInput())

    Console.println("Closing subject type, should close the queries")
    await(SubjectLibrary.Close(testSubjectName))
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

  "Kafka-Sinks" should " be able to recieve data from multiple sinks" taggedAs (Slow, KafkaTest) in async {
    await(Future.sequence(for (_ <- 1 to 3) yield {
      CreateTestInput()
    }))

    val environments = Future.sequence(Seq(CreateSourceQuery(1),CreateSourceQuery(2) ,CreateSourceQuery(3)))

    Console.println("Closing subject type, should close the queries")
    await(SubjectLibrary.Close(testSubjectName))
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
      val env = StreamExecutionEnvironment.createLocalEnvironment()
      env.setParallelism(parallelism)
      Await.ready(createTopology(env, nr),Duration(120, SECONDS))
      logger.debug(s"Starting environment $nr")
      env.execute(s"job$nr")
      logger.debug(s"Environment $nr finished")
    }

    /**
      * Create a simple topology that converts the integer object to a string
      * @param env Stream Execution Environment to create topology on
      * @param nr Number used to identify topology in the test
      */
    def createTopology(env: StreamExecutionEnvironment, nr: Int): Future[Unit] = async {
      //Construct a new source using the subjectFactory
      val subjectType = await(SubjectLibrary.GetOrCreateType[MyOwnIntegerObject]())
      //Transient lazy because these need to be initialized at the distributed environment
      val unMapper = SubjectFactory.GetUnTransformer[MyOwnIntegerObject](subjectType)
      val source = SubjectFactory.GetSource(subjectType)
      env
        .addSource(source)
        .map(unMapper)
        .map(o => Tuple2(nr, o))
        .addSink(o => TestCollector.collect(o))
    }
  }
}
