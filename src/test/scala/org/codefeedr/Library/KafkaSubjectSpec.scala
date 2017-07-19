package org.codefeedr.Library

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.Library.Internal.KafkaController
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, Matchers}
import org.apache.flink.streaming.api.scala._

import scala.concurrent.Future

@SerialVersionUID(100L)
case class MyOwnIntegerObject(value: Int) extends Serializable

/**
  * Created by Niels on 14/07/2017.
  */
class KafkaSubjectSpec extends AsyncFlatSpec with Matchers with BeforeAndAfterAll with LazyLogging {
  //These tests must run in parallel
  implicit override def executionContext = scala.concurrent.ExecutionContext.Implicits.global

  "A KafkaSource" should "retrieve all messages published by a source" in {
    //val env = StreamExecutionEnvironment.getExecutionEnvironment
    val sinkF = KafkaSubjectFactory.GetSink[MyOwnIntegerObject]
    sinkF.flatMap(sink => {
      val t1 = new Thread(new MyOwnSink(1))
      t1.start()
      val t2 = new Thread(new MyOwnSink(2))
      t2.start()
      val t3 = new Thread(new MyOwnSink(3))
      t3.start()

      Thread.sleep(15000)
      logger.debug("Sending events")
      sink.invoke(MyOwnIntegerObject(1))
      sink.invoke(MyOwnIntegerObject(2))
      sink.invoke(MyOwnIntegerObject(3))

      Thread.sleep(2000)

      assert(true)
      KafkaLibrary.UnRegisterSubject("MyOwnIntegerObject").map(_ => assert(true))

    })

  }

  class MyOwnSink(nr: Int) extends Runnable with LazyLogging {
    override def run(): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      val sourceF = KafkaSubjectFactory.GetSource[MyOwnIntegerObject]
      val number = nr //Copy to this scope, otherwise it is not copied by flink
      logger.debug(s"Creating environment $nr")

      val job = sourceF
        .map(source => {
          val mapped = env
            .addSource[MyOwnIntegerObject](source)
            .map { o =>
              s"Value in environment $number is ${o.value}"
            }
          //    mapped.addSink(o => logger.debug(o))
          mapped.print()
        })

      while (!job.isCompleted) {
        Thread.sleep(10)
      }

      logger.debug(s"Starting environment $nr")
      env.execute(s"job${nr}")
    }
  }
}
