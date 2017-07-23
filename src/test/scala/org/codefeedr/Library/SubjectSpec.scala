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

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, Matchers}
import org.apache.flink.streaming.api.scala._
import org.codefeedr.Library.Internal.Kafka.KafkaController

import scala.collection.mutable
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.collection.mutable.Set

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
  * Created by Niels on 14/07/2017.
  */
class KafkaSubjectSpec extends AsyncFlatSpec with Matchers with BeforeAndAfterAll with LazyLogging {
  //These tests must run in parallel
  implicit override def executionContext: ExecutionContextExecutor =
    scala.concurrent.ExecutionContext.Implicits.global

  "Kafka-Sinks" should "retrieve all messages published by a source" in {
    //Create a sink function
    val sinkF = SubjectFactory.GetSink[MyOwnIntegerObject]
    sinkF.flatMap(sink => {
      val env = StreamExecutionEnvironment.createLocalEnvironment()

      val t1 = new Thread(new MyOwnSourseQuery(1))
      t1.start()
      val t2 = new Thread(new MyOwnSourseQuery(2))
      t2.start()
      val t3 = new Thread(new MyOwnSourseQuery(3))
      t3.start()

      env.fromCollection(mutable.Set(1, 2, 3).toSeq).map(o => MyOwnIntegerObject(o)).addSink(sink)
      env.execute("sink")

      Thread.sleep(3000)

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

    val t1 = new Thread(new MyOwnSourseQuery(1))
    t1.start()
    val t2 = new Thread(new MyOwnSourseQuery(2))
    t2.start()
    val t3 = new Thread(new MyOwnSourseQuery(3))
    t3.start()

    Thread.sleep(500)

    //Create a sink function
    val sinkF = SubjectFactory.GetSink[MyOwnIntegerObject]
    sinkF.flatMap(sink => {
      val env = StreamExecutionEnvironment.createLocalEnvironment()
      env.fromCollection(mutable.Set(1, 2, 3).toSeq).map(o => MyOwnIntegerObject(o)).addSink(sink)
      env.execute("sink")

      //Wait for kafka
      Thread.sleep(3000)

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

    val t1 = new Thread(new MyOwnSourseQuery(1))
    t1.start()
    val t2 = new Thread(new MyOwnSourseQuery(2))
    t2.start()
    val t3 = new Thread(new MyOwnSourseQuery(3))
    t3.start()

    Thread.sleep(500)

    Future
      .sequence(for (i <- 1 to 3) yield {
        val sinkF = SubjectFactory.GetSink[MyOwnIntegerObject]
        sinkF
          .map(sink => {
            val env = StreamExecutionEnvironment.createLocalEnvironment()
            env
              .fromCollection(mutable.Set(1, 2, 3).toSeq)
              .map(o => MyOwnIntegerObject(o))
              .addSink(sink)
            env.execute("sink")
          })
      })
      .flatMap(_ => {
        //Wait for kafka data to be retrieved
        Thread.sleep(3000)
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
      val sourceF = SubjectFactory.GetSource[MyOwnIntegerObject]
      sourceF.map(source => {
        val mapped = env
          .addSource[MyOwnIntegerObject](source)
          .map { o =>
            Tuple2(nr, o)
          }
          .addSink(o => TestCollector.collect(o))
      })

    }
  }
}
