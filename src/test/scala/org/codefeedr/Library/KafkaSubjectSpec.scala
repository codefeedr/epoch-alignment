package org.codefeedr.Library

import org.codefeedr.Library.Internal.KafkaController
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, Matchers}

import org.apache.flink.streaming.api.scala._

import scala.concurrent.Future

case class MyOwnIntegerObject(value: Int)

/**
  * Created by Niels on 14/07/2017.
  */
class KafkaSinkSpec extends AsyncFlatSpec with Matchers with BeforeAndAfterAll {

  override def beforeAll(): Unit = {

    val f = KafkaLibrary.Initialize()
    while (f.value.isEmpty) {
      Thread.sleep(100)
    }
  }

  override def afterAll(): Unit = {
    val f = KafkaLibrary.Shutdown()
    while (f.value.isEmpty) {
      Thread.sleep(100)
    }
  }

  "A KafkaSink" should "be able to create a topic for itself" in {
    KafkaLibrary
      .GetType[MyOwnIntegerObject]()
      .flatMap(subjectType => {
        new KafkaSink[MyOwnIntegerObject](subjectType)
          .invoke(MyOwnIntegerObject(1))
        KafkaController
          .GetTopics()
          .flatMap(o => {
            assert(o.contains(s"${subjectType.name}_${subjectType.uuid}"))
          })
      })
  }

  "A KafkaSource" should "retrieve all messages published by a source" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val sourceF = KafkaSubjectFactory.GetSource[MyOwnIntegerObject]
    val sinkF = KafkaSubjectFactory.GetSink[MyOwnIntegerObject]
    (for {
      source <- sourceF
      sink <- sinkF
    } yield (source, sink)).flatMap(subject => {
      val source = env.addSource[MyOwnIntegerObject](subject._1)

      val mapped = source
        .map { o =>
          s"Integer value is ${o.value}"
        }
      mapped.print

      env.execute()

      subject._2.invoke(MyOwnIntegerObject(1))
      subject._2.invoke(MyOwnIntegerObject(2))
      subject._2.invoke(MyOwnIntegerObject(3))
      Future {
        Thread.sleep(10000)
        assert(true)
      }
    })

  }
}
