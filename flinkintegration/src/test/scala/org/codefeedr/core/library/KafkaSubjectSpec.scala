

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

package org.codefeedr.core.library

import java.util.UUID
import java.util.concurrent.Executors

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.CheckpointingMode
import org.scalatest._
import org.apache.flink.streaming.api.scala._
import org.codefeedr.core.library.internal.SubjectTypeFactory
import org.codefeedr.core.{FullIntegrationSpec, IntegrationTestLibraryServices, KafkaTest}
import org.codefeedr.core.library.internal.zookeeper.{ZkClient, ZkClientComponent}
import org.codefeedr.core.library.metastore.SubjectLibraryComponent
import org.codefeedr.model.TrailedRecord
import org.scalatest.tagobjects.Slow
import org.slf4j.MDC

import scala.collection.mutable
import scala.concurrent.{TimeoutException, _}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}
import scala.util.{Failure, Success}


@SerialVersionUID(100L)
case class MyOwnIntegerObject(value: Int) extends Serializable


object TestCollector extends LazyLogging {
  var collectedData: mutable.MutableList[(Int, MyOwnIntegerObject)] =
    mutable.MutableList[(Int, MyOwnIntegerObject)]()


  def collect(nr:Int)(myOwnIntegerObject: MyOwnIntegerObject): Unit = {
    logger.info(s"${nr} received ${myOwnIntegerObject}")
    this.synchronized {
      collectedData += Tuple2(nr, myOwnIntegerObject)
    }
  }

  //Clear the collected data
  def reset():Unit = {
    collectedData = mutable.MutableList[(Int, MyOwnIntegerObject)]()
  }
}



/**
  * Integration tests that tests the functionality of a plugin
  * Do not take this class as an example how to write tests for query functionality
  * See JoinQuerySpec for a better example
  * Created by Niels on 14/07/2017.
  */
class KafkaSubjectSpec extends FullIntegrationSpec with BeforeAndAfterEach {
  val testSubjectName = "MyOwnIntegerObject"

  override val libraryServices: MyOwnSourceQueryComponent with Object = new MyOwnSourceQueryComponent{}

  def CreateSourceQuery(nr: Int): Future[Unit] = {
    Future {
      blocking {
        libraryServices.CreateSourceQuery(nr,parallelism).run()
      }
    }
  }

  override def beforeEach(): Unit = super.beforeEach()

  override def afterEach(): Unit = {
    super.afterEach()
    TestCollector.reset()
  }


  "Kafka-Sources" should "retrieve all messages published by a source" taggedAs(Slow, KafkaTest) in async {
    val subjectNode = libraryServices.subjectLibrary.getSubject(testSubjectName)
    assert(!await(subjectNode.exists()))
    //Generate some test input
    await(runSourceEnvironment[MyOwnIntegerObject](mutable.Set(1, 2, 3).map(o => MyOwnIntegerObject(o)).toArray))
    //Creating fake query environments
    val environments = Future.sequence(Seq(CreateSourceQuery(1), CreateSourceQuery(2), CreateSourceQuery(3)))

    Console.println("Closing subject type, should close the queries")
    await(subjectNode.setState(false))
    Console.println("Waiting for completion of queries")
    await(environments)
    println("Completed")

    //Clean up subject
    await(subjectNode.unregister())

    //Assert results
    assert(TestCollector.collectedData.count(o => o._1 == 1) == 3)
    assert(TestCollector.collectedData.count(o => o._1 == 2) == 3)
    assert(TestCollector.collectedData.count(o => o._1 == 3) == 3)
    assert(TestCollector.collectedData.count(o => o._2.value == 1) == 3)
    assert(TestCollector.collectedData.count(o => o._2.value == 2) == 3)
    assert(TestCollector.collectedData.count(o => o._2.value == 3) == 3)
  }


  it should "still receive data if they are created before the sink" taggedAs(Slow, KafkaTest) in async {
    val subjectNode = libraryServices.subjectLibrary.getSubject(testSubjectName)
    assert(!await(subjectNode.exists()))

    //Generate some test input
    await(runSourceEnvironment[MyOwnIntegerObject](mutable.Set(1, 2, 3).map(o => MyOwnIntegerObject(o)).toArray))

    //Creating fake query environments
    val environments = Future.sequence(Seq(CreateSourceQuery(1), CreateSourceQuery(2), CreateSourceQuery(3)))

    Console.println("Closing subject type, should close the queries")
    await(subjectNode.setState(false))
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

  /*
  it should "be able to receive data from multiple sinks" taggedAs(Slow, KafkaTest) in async {
    val subjectNode = subjectLibrary.getSubject(testSubjectName)
    assert(!await(subjectNode.exists()))
    //Create the subject here, to prevent the multiple source environments from racing on creating it
    await(subjectFactory.create[MyOwnIntegerObject]())
    await(Future.sequence(for (_ <- 1 to 3) yield {
      runSourceEnvironment[MyOwnIntegerObject](mutable.Set(1, 2, 3).map(o => MyOwnIntegerObject(o)).toArray)
    }))

    val environments = Future.sequence(Seq(CreateSourceQuery(1), CreateSourceQuery(2), CreateSourceQuery(3)))

    Console.println("Closing subject type, should close the queries")
    await(subjectNode.setState(false))
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
  */
}

trait MyOwnSourceQueryComponent extends IntegrationTestLibraryServices {


  def CreateSourceQuery(nr:Int, parallelism:Int):MyOwnSourceQuery =
    new MyOwnSourceQuery(nr, parallelism)

class MyOwnSourceQuery(nr: Int, parallelism: Int)
  extends Runnable
    with LazyLogging {

  override def run(): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism)
    env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
    val topology = createTopology(env, nr)
    Await.ready(topology, Duration(120, SECONDS))
    topology.value match {
      case Some(Success(x)) => println(s"Topology has been created")
      case Some(Failure(e)) => throw e
      case _ => throw new Exception("Cannot get here")
    }

    MDC.put("envId", s"source-$nr")
    logger.debug(s"Starting environment $nr")
    env.execute(s"job$nr")
    logger.debug(s"Environment $nr finished")
    MDC.remove("envId")
  }

  /**
    * Create a simple topology that converts records back into MyOwnIntegerObjects and passes it to a testcollecter
    *
    * @param env Stream Execution Environment to create topology on
    * @param nr  Number used to identify topology in the test
    */

  def createTopology(env: StreamExecutionEnvironment, nr: Int): Future[Unit] = async {
    val name = UUID.randomUUID().toString
    val jobNode = subjectLibrary.getJob(name)

    val subjectType = SubjectTypeFactory.getSubjectType[MyOwnIntegerObject]
    val subjectNode = subjectLibrary.getSubject(subjectType.name)

    //val transformer = Library.subjectFactory.getUnTransformer[MyOwnIntegerObject](subjectType)
    val source = subjectFactory.getSource[MyOwnIntegerObject](subjectNode, jobNode, s"testSource_$nr")
    val r = () => {
      val num = nr
      env
        .addSource(source)
        .addSink(o => TestCollector.collect(num)(o))
    }
    r()
  }
}
}
