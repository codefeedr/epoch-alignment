package org.codefeedr.experiments

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.codefeedr.core.library.CodefeedrComponents
import org.apache.flink.runtime.state.filesystem.FsStateBackend

trait ExperimentBase extends CodefeedrComponents {
  protected def getWindowTime: Time =
    Time.seconds(configurationProvider.getInt("window.size", Some(10)))

  protected def getStateBackendPath: String = configurationProvider.get("statebackend.path")

  def initialize(args: Array[String]) = {
    val pt = ParameterTool.fromArgs(args)
    configurationProvider.initConfiguration(pt)
  }

  def getStateBackend: StateBackend =
    new FsStateBackend(getStateBackendPath, true)

  /**
    * Creates a new stream stream execution environment with some default configuration
    * @return
    */
  protected def getEnvironment: StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.setStateBackend(getStateBackend)
    env.setParallelism(16)
    env
  }
}
