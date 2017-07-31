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

package org.codefeedr.Library

import java.util.concurrent.Executors

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, Matchers}
import org.apache.flink.streaming.api.scala._
import scala.collection.mutable
import scala.concurrent.{TimeoutException, _}
import scala.concurrent.duration._

@SerialVersionUID(100L)
case class MyOwnIntegerObject(value: Int) extends Serializable

object TestCollector extends LazyLogging {
  var collectedData: mutable.MutableList[(Int, MyOwnIntegerObject)] =
    mutable.MutableList[Tuple2[Int, MyOwnIntegerObject]]()

  def collect(item: Tuple2[Int, MyOwnIntegerObject]): Unit = {
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
class KafkaSubjectSpec extends AsyncFlatSpec with Matchers with BeforeAndAfterAll with LazyLogging {
  //These tests must run in parallel
  implicit override def executionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutorService(Executors.newWorkStealingPool(16))

  val paralellism = 2

  "Kafka-Sinks" should "retrieve all messages published by a source" in {
    //Create a sink function
    val sinkF = SubjectFactory.GetSink[MyOwnIntegerObject]
    sinkF.flatMap(sink => {
      val env = StreamExecutionEnvironment.createLocalEnvironment()
      env.setParallelism(paralellism)
      env.fromCollection(mutable.Set(1, 2, 3).toSeq).map(o => MyOwnIntegerObject(o)).addSink(sink)
      env.execute()

      val environments = for {
        f1 <- Future { new MyOwnSourseQuery(1).run() }
        f2 <- Future { new MyOwnSourseQuery(2).run() }
        f3 <- Future { new MyOwnSourseQuery(3).run() }
      } yield (f1, f2, f3)

      Console.println("Waiting for completion")
      try {
        Await.result(environments, Duration(3, SECONDS))
      } catch {
        case _: TimeoutException => Unit
      }
      Thread.sleep(5000)
      Console.println("Completed")
      //Delete the subject
      SubjectLibrary
        .UnRegisterSubject("MyOwnIntegerObject")
        .map(_ => {
          assert(TestCollector.collectedData.count(o => o._1 == 1) == 3)
          assert(TestCollector.collectedData.count(o => o._1 == 2) == 3)
          assert(TestCollector.collectedData.count(o => o._1 == 3) == 3)
          assert(TestCollector.collectedData.count(o => o._2.value == 1) == 3)
          assert(TestCollector.collectedData.count(o => o._2.value == 2) == 3)
          assert(TestCollector.collectedData.count(o => o._2.value == 3) == 3)
        })
    })
  }

  "Kafka-Sinks" should " still receive data if they are created before the sink" in {
    //Reset the cache
    TestCollector.collectedData = mutable.MutableList[Tuple2[Int, MyOwnIntegerObject]]()

    val environments = for {
      f1 <- Future { new MyOwnSourseQuery(1).run() }
      f2 <- Future { new MyOwnSourseQuery(2).run() }
      f3 <- Future { new MyOwnSourseQuery(3).run() }
    } yield (f1, f2, f3)

    //Wait for kafka
    try {
      Await.result(environments, Duration(1, SECONDS))
    } catch {
      case _: TimeoutException => Unit
    }

    //Create a sink function
    val sinkF = SubjectFactory.GetSink[MyOwnIntegerObject]
    sinkF.flatMap(sink => {
      val env = StreamExecutionEnvironment.createLocalEnvironment()
      env.setParallelism(paralellism)
      env.fromCollection(mutable.Set(1, 2, 3).toSeq).map(o => MyOwnIntegerObject(o)).addSink(sink)
      env.execute("sink")

      Thread.sleep(8000)

      //Delete the subject as cleanup
      SubjectLibrary
        .UnRegisterSubject("MyOwnIntegerObject")
        .map(_ => {
          assert(TestCollector.collectedData.count(o => o._1 == 1) == 3)
          assert(TestCollector.collectedData.count(o => o._1 == 2) == 3)
          assert(TestCollector.collectedData.count(o => o._1 == 3) == 3)
          assert(TestCollector.collectedData.count(o => o._2.value == 1) == 3)
          assert(TestCollector.collectedData.count(o => o._2.value == 2) == 3)
          assert(TestCollector.collectedData.count(o => o._2.value == 3) == 3)
        })
    })
  }

  "Kafka-Sinks" should " be able to recieve data from multiple sinks" in {
    //Reset the cache
    TestCollector.collectedData = mutable.MutableList[Tuple2[Int, MyOwnIntegerObject]]()

    Future
      .sequence(for (i <- 1 to 3) yield {
        val sinkF = SubjectFactory.GetSink[MyOwnIntegerObject]
        sinkF
          .map(sink => {
            val env = StreamExecutionEnvironment.createLocalEnvironment()
            env.setParallelism(paralellism)
            env
              .fromCollection(mutable.Set(1, 2, 3).toSeq)
              .map(o => MyOwnIntegerObject(o))
              .addSink(sink)
            env.execute("sink")
          })
      })
      .flatMap(_ => {

        val environments = for {
          f1 <- Future { new MyOwnSourseQuery(1).run() }
          f2 <- Future { new MyOwnSourseQuery(2).run() }
          f3 <- Future { new MyOwnSourseQuery(3).run() }
        } yield (f1, f2, f3)
        //Wait for kafka data to be retrieved

        Console.println("Waiting for completion")
        try {
          Await.result(environments, Duration(7, SECONDS))
        } catch {
          case _: TimeoutException => Unit
        }
        Thread.sleep(9000)
        Console.println("Completed")

        //Delete the subject as cleanup
        SubjectLibrary
          .UnRegisterSubject("MyOwnIntegerObject")
          .map(_ => {
            assert(TestCollector.collectedData.count(o => o._1 == 1) == 9)
            assert(TestCollector.collectedData.count(o => o._1 == 2) == 9)
            assert(TestCollector.collectedData.count(o => o._1 == 3) == 9)
            assert(TestCollector.collectedData.count(o => o._2.value == 1) == 9)
            assert(TestCollector.collectedData.count(o => o._2.value == 2) == 9)
            assert(TestCollector.collectedData.count(o => o._2.value == 3) == 9)
          })
      })
  }

  class MyOwnSourseQuery(nr: Int) extends Runnable with LazyLogging {
    override def run(): Unit = {
      val env = StreamExecutionEnvironment.createLocalEnvironment()
      env.setParallelism(paralellism)
      createTopology(env, nr).map(_ => {
        logger.debug(s"Starting environment $nr")
        env.execute(s"job$nr")
      })

    }

    /**
      * Create a simple topology that converts the integer object to a string
      * @param env Stream Execution Environment to create topology on
      * @param nr Number used to identify topology in the test
      */
    def createTopology(env: StreamExecutionEnvironment, nr: Int): Future[Unit] = {
      //Construct a new source using the subjectFactory
      SubjectLibrary.GetType[MyOwnIntegerObject]().map(subjectType => {
        //Transient lazy because these need to be initioalised at the distributed environment
        val unMapper = SubjectFactory.GetUnTransformer[MyOwnIntegerObject](subjectType)
        val source = SubjectFactory.GetSource(subjectType)
        env
          .addSource(source)
          .map(unMapper)
          .map(o =>Tuple2(nr, o))
          .addSink(o => TestCollector.collect(o))
      })

    }
  }
}
