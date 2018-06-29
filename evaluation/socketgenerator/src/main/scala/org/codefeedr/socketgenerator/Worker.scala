package org.codefeedr.socketgenerator

import java.io.{BufferedWriter, OutputStreamWriter, PrintWriter}
import java.net.ServerSocket
import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import scala.concurrent.ExecutionContext.Implicits.global
import resource.managed

import scala.concurrent.{CancellationException, Future, blocking}


class Worker(@volatile var rate: Int, port: Int) {
  private var future: ScheduledFuture[_] = _
  @volatile var producedElements: Long = 0
  @volatile var running: Boolean = true
  val r = scala.util.Random

  def setRate(newRate: Int): Unit = {
    rate = newRate
  }

  def run(): Future[Unit] = Future {
    blocking {
      if (future != null && future.isDone) {
        throw new IllegalStateException(s"Worker is already running")
      }
      println(s"Started generating events loop. Waiting for connection")
      try {
        for {
          server <- managed(new ServerSocket(port))
          connection <- managed(server.accept)
          outStream <- managed(
            new PrintWriter(
              new BufferedWriter(new OutputStreamWriter(connection.getOutputStream))))
          ex <- managed(new ClosableScheduledThreadPoolExecutor(1))
        } {
          println(s"Connection opened")
          val task = new Runnable {
            def run(): Unit = {
              val loopRate = rate
              println(s"Writing $loopRate elements.")
              for (i <- 1 to loopRate) {
                outStream.write(s"${r.nextInt()}|${r.nextInt()}\n")
              }
              outStream.flush()
              producedElements += loopRate
            }
          }
          future = ex.scheduleAtFixedRate(task, 1, 1, TimeUnit.SECONDS)
          try {
            future.get()
          } catch {
            case e: CancellationException => ()
          }
          println("Generator task finished")
        }
      } catch {
        case e: Exception => println(e)
      } finally {
        println("worker run finished")
      }
    }
  }

  def finish(): Unit = {
    future.cancel(true)
  }
}


/**
  * Simple extension to the ScheduledThreadPoolExecutor, which calls shutdown upon close so it can be used with ARM
  * @param corePoolSize the number of threads to keep in the pool, even
  *                                 if they are idle, unless { @code allowCoreThreadTimeOut} is set
  */
class ClosableScheduledThreadPoolExecutor(corePoolSize: Int)
  extends ScheduledThreadPoolExecutor(corePoolSize) {
  def close(): Unit = shutdown()
}