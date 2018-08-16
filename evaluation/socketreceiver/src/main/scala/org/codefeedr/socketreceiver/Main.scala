package org.codefeedr.socketreceiver

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._

import scala.concurrent.duration._
import org.codefeedr.core.library.LibraryServices
import org.codefeedr.evaluation.IntTuple
import org.slf4j.MDC

import scala.concurrent.Await

object Main extends LazyLogging{
  def main(args: Array[String]): Unit = {
    val runIncrement = Option(System.getenv("RUN_INCREMENT"))



    if(runIncrement.isEmpty) {
      logger.warn("No run increment was defined")
    }

    //Put runIdentifier in MDC so we can filter results in ELK.
    MDC.put("RUN_INCREMENT", runIncrement.getOrElse("-1"))
    //TODO: Find some better name
    MDC.put("JOB_IDENTIFIER", "SOCKETRECEIVER")

    // the port to connect to
    val port = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'")
        throw e
      }
    }

    val hostname = try {
      ParameterTool.fromArgs(args).get("hostname")
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount --hostname <hostname>'")
        throw e
      }
    }

    LibraryServices.zkClient.deleteRecursive("/")
    LibraryServices.subjectLibrary.initialize()
    val subject = Await.result(LibraryServices.subjectFactory.create[IntTuple](), 10.seconds)
    val sink = Await.result(LibraryServices.subjectFactory.getSink[IntTuple]("testsink", "myjob"),
      10.seconds)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(1000,CheckpointingMode.EXACTLY_ONCE)
    env
      .socketTextStream(hostname, port)
      .map(toIntTuple(_))
      .addSink(sink)
    env.execute()

  }

  def toIntTuple(s: String): IntTuple = {
    val r = s.split('|')
    IntTuple(r(0).toInt, r(1).toInt)
  }
}
