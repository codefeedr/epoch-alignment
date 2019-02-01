package org.codefeedr.socketgenerator

import scala.concurrent.Await
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.slf4j.MDC


object Main extends LazyLogging{

  def main(args: Array[String]): Unit = {
    logger.info(s"Starting generation at port ${config.port}with a rate of ${config.rate}")

    val runIncrement = Option(System.getenv("RUN_INCREMENT"))

    if(runIncrement.isEmpty) {
      logger.warn("No run increment was defined")
    }

    //Put runIdentifier in MDC so we can filter results in ELK.
    MDC.put("RUN_INCREMENT", runIncrement.getOrElse("-1"))
    //TODO: Find some better name
    MDC.put("JOB_IDENTIFIER", "SOCKETGENERATOR")



    val worker = new Worker(config)
    val f = worker.run()
    //Check if we should stop
    while (worker.producedElements < config.totalEvents) {
      Thread.sleep(10)
    }
    worker.finish()
    Await.result(f, 10.second)
    logger.info(s"closing application after ${worker.producedElements} events")
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
