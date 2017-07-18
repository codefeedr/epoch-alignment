package org.codefeedr

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.codefeedr.Library.Internal.KafkaController
import org.codefeedr.Library.{KafkaLibrary, KafkaSink, SinkFactory}
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, FlatSpec, Matchers}
import org.apache.flink.api.scala._

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

}
