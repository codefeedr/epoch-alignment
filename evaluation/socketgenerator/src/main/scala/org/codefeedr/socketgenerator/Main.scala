package org.codefeedr.socketgenerator

import scala.concurrent.Await
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory

object Main {

  def main(args: Array[String]): Unit = {
    println(s"Starting generation at port ${config.port}with a rate of ${config.rate}")

    val worker = new Worker(config)
    val f = worker.run()
    //Check if we should stop
    while (worker.producedElements < config.totalEvents) {
      Thread.sleep(10)
    }
    worker.finish()
    Await.result(f, 10.second)
    println(s"closing application after ${worker.producedElements} events")
  }



  private lazy val config = {
    val conf = ConfigFactory.load().getConfig("socketGenerator")
    SocketGeneratorConfig(
      conf.getString("hostname"),
      conf.getInt("port"),
      conf.getInt("backlog"),
      conf.getInt("rate"),
      conf.getInt("totalEvents")
    )
  }


}
