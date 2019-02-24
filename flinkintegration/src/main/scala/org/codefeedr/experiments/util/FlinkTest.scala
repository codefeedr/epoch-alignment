package org.codefeedr.experiments.util
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{
  TumblingEventTimeWindows,
  TumblingProcessingTimeWindows
}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger

case class value(i: Int)

object FlinkTest {

  def main(args: Array[String]): Unit = {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    val numbers = env
      .fromCollection(1 to 10)
      .map(o => value(o))
      .keyBy(o => "1")
      .window(TumblingProcessingTimeWindows.of(Time.seconds(100000)))
      .reduce((a, b) => {
        Console.println(s"left: $a, right: $b")
        value(a.i + b.i)
      })
      .addSink(o => Console.println(o))

    // execute program
    env.execute("Streaming WordCount")

  }
}
