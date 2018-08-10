package org.codefeedr.socketgenerator

import scala.concurrent.Await
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.slf4j.MDC


object Main extends LazyLogging{

  def main(args: Array[String]): Unit = {
    MDC.put("Testnumber", "1000")
    logger.info(s"Starting generation at port ${config.port}with a rate of ${config.rate}")

    val worker = new Worker(config)
    val f = worker.run()
    //Check if we should stop
    while (worker.producedElements < config.totalEvents) {
      Thread.sleep(10)
    }
    worker.finish()
    Await.result(f, 10.second)
    logger.info(s"closing application after ${worker.producedElements} events")
    MDC.remove("Testnumber")
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
