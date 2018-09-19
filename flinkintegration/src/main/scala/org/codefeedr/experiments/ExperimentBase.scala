package org.codefeedr.experiments

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.codefeedr.core.library.CodefeedrComponents

trait ExperimentBase extends CodefeedrComponents {
  protected val propertiesFileName: Option[String] = None

  protected def getWindowTime: Time =
    Time.seconds(configurationProvider.getInt("window.size", Some(10)))

  def initialize(args: Array[String]) = {
    val pt = ParameterTool.fromArgs(args)
    configurationProvider.initConfiguration(pt, propertiesFileName)
  }

  /**
    * Creates a new stream stream execution environment with some default configuration
    * @return
    */
  protected def getEnvironment: StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env
  }

}
