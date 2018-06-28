package org.codefeedr.jobs

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.codefeedr.core.library.LibraryServices
import org.codefeedr.core.library.internal.kafka.sink.KafkaGenericSink
import org.codefeedr.model.IntTuple

import scala.concurrent._
import scala.concurrent.duration._

object LongTupleInputJob {
  def main(args: Array[String]): Unit = {

    // the port to connect to
    val port = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'")
        return
      }
    }

    val hostname = try {
      ParameterTool.fromArgs(args).get("hostname")
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'")
        return
      }
    }

    LibraryServices.subjectLibrary.initialize()
    val subject = Await.result(LibraryServices.subjectFactory.create[IntTuple](), 10.seconds)
    val sink = Await.result(LibraryServices.subjectFactory.getSink[IntTuple]("testsink", "myjob"),
                            10.seconds)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
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
