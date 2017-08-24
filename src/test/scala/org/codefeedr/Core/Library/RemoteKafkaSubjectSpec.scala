package org.codefeedr.Core.Library

import java.util.concurrent.Executors

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.codefeedr.Core.KafkaTest
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, Matchers}
import org.scalatest.tagobjects.Slow
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration.{Duration, SECONDS}



@SerialVersionUID(100L)
case class RemoteKafkaSubjectTestInt(value: Int) extends Serializable


  /**
    * Collector used to validate the test
    */
  object TestRemoteCollector extends LazyLogging {
    var collectedData: mutable.MutableList[(Int, RemoteKafkaSubjectTestInt)] =
      mutable.MutableList[Tuple2[Int, RemoteKafkaSubjectTestInt]]()

    def collect(item: Tuple2[Int, RemoteKafkaSubjectTestInt]): Unit = {
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
  /*
  class RemoteKafkaSubjectSpec extends AsyncFlatSpec with Matchers with BeforeAndAfterAll {
    //These tests must run in parallel
    implicit override def executionContext: ExecutionContextExecutor =
      ExecutionContext.fromExecutorService(Executors.newWorkStealingPool(16))

    //Flink cluster configuration
    val conf: Config = ConfigFactory.load
    val host: String = conf.getString("codefeedr.flink.remote.host")
    val port: Int = conf.getInt("codefeedr.flink.remote.port")
    val jars: Array[String] = conf.getString("codefeedr.flink.remote.jars").split(',')
    val parallelism: Int = conf.getInt("codefeedr.flink.remote.parallelism")

    "Kafka-Sinks" should "retrieve all messages published by a source" taggedAs (Slow, KafkaTest) in {
      //Create a sink function
      val sinkF = SubjectFactory.GetSink[RemoteKafkaSubjectTestInt]
      sinkF.flatMap(sink => {
        val env = StreamExecutionEnvironment.createRemoteEnvironment(host, port, parallelism,jars:_*)
        env.fromCollection(mutable.Set(1, 2, 3).toSeq).map(o => {
          val logger =LoggerFactory.getLogger("TestToKafka")
          logger.warn(s"Handling event $o")
          logger.info(s"Handling event $o")
          logger.error(s"Handling event $o")
          RemoteKafkaSubjectTestInt(o)
        }).addSink(sink)
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
        Thread.sleep(10000)
        Console.println("Completed")
        //Delete the subject
        SubjectLibrary
          .UnRegisterSubject("RemoteKafkaSubjectTestInt")
          .map(_ => {
            assert(TestRemoteCollector.collectedData.count(o => o._1 == 1) == 3)
            assert(TestRemoteCollector.collectedData.count(o => o._1 == 2) == 3)
            assert(TestRemoteCollector.collectedData.count(o => o._1 == 3) == 3)
            assert(TestRemoteCollector.collectedData.count(o => o._2.value == 1) == 3)
            assert(TestRemoteCollector.collectedData.count(o => o._2.value == 2) == 3)
            assert(TestRemoteCollector.collectedData.count(o => o._2.value == 3) == 3)
          })
      })
    }
    */
/*
    "Kafka-Sinks" should " still receive data if they are created before the sink" taggedAs (Slow, KafkaTest) in {
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

    "Kafka-Sinks" should " be able to recieve data from multiple sinks" taggedAs (Slow, KafkaTest) in {
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
    */
/*
    class MyOwnSourseQuery(nr: Int) extends Runnable with LazyLogging {
      override def run(): Unit = {
      //  val env = StreamExecutionEnvironment.createRemoteEnvironment(host, port, parallelism,jars:_*)
        val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism)
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
        SubjectLibrary
          .GetOrCreateType[RemoteKafkaSubjectTestInt]()
          .map(subjectType => {
            //Transient lazy because these need to be initioalised at the distributed environment
            val unMapper = SubjectFactory.GetUnTransformer[RemoteKafkaSubjectTestInt](subjectType)
            val source = SubjectFactory.GetSource(subjectType)
            env
              .addSource(source)
              .map(unMapper)
              .map(o => Tuple2(nr, o))
              .addSink(o => TestRemoteCollector.collect(o))
          })

      }
    }
  }
*/



