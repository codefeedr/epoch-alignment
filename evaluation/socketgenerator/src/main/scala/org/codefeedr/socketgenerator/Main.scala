package org.codefeedr.socketgenerator

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try
import com.typesafe.config.ConfigFactory

object Main {

  def main(args: Array[String]): Unit = {
    println(s"Starting generation at port ${config.port}with a rate of ${config.rate}")

    val worker = new Worker(config.rate, config.port)
    val f = worker.run()

    var running: Boolean = true

    while (running) {
      val line = scala.io.StdIn.readLine()
      if (line.toLowerCase == "stop" || line.toLowerCase == "quit") {
        println(s"Stopping generation at port ${config.port}")
        worker.finish()
        Await.result(f, 10.seconds)
        running = false
      } else {
        Try(line.toInt).toOption match {
          case None => println(s"Unknown command: $line")
          case Some(i: Int) =>
            worker.setRate(i)
            println(s"Setting rate to $line")
        }
      }
    }
    println("closing application")
  }



  private lazy val config = {
    val conf = ConfigFactory.load().getConfig("socketGenerator")
    SocketGeneratorConfig(
      conf.getInt("port"),
      conf.getInt("rate"),
      conf.getInt("totalEvents")
    )
  }


}
