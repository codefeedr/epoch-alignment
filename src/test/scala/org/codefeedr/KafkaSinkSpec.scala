package org.codefeedr

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.codefeedr.Library.Internal.KafkaController
import org.codefeedr.Library.KafkaSink
import org.scalatest.{AsyncFlatSpec, FlatSpec, Matchers}
import org.apache.flink.api.scala._

case class MyOwnIntegerObject(value: Int)

/**
  * Created by Niels on 14/07/2017.
  */
class KafkaSinkSpec extends AsyncFlatSpec with Matchers {
  /*
  "A KafkaSink" should "be able to create a topic for itself" in {
    val sink = new KafkaSink[MyOwnIntegerObject]
    sink.invoke(MyOwnIntegerObject(1))
    KafkaController
      .GetTopics()
      .flatMap(o => {
        assert(o.contains("MyOwnIntegerObject"))
      })
  }

  "A KafkaSink" should "be able to be removed again" in {
    for {
      _ <- KafkaController.Destroy()
      empty <- KafkaController.GetTopics().map(o => assert(o.size == 0))
    } yield empty
  }
 */
}
