package org.codefeedr.experiments

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.codefeedr.core.library.CodefeedrComponents
import org.apache.flink.runtime.state.filesystem.FsStateBackend

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}

trait ExperimentBase extends CodefeedrComponents {

  protected def getStateBackendPath: String = configurationProvider.get("statebackend.path")

  protected def getParallelism: Int = 2
  protected def getKafkaParallelism: Int = 6

  @transient protected lazy val getEnvironment: StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    configurationProvider.initEc(env.getConfig)
    //env.getConfig.disableGenericTypes()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.setStateBackend(getStateBackend)
    env.setRestartStrategy(
      RestartStrategies.fixedDelayRestart(
        0,
        Time.seconds(60)
      ))
    env.setParallelism(getParallelism)
    env
  }

  def initialize(args: Array[String]) = {
    val pt = ParameterTool.fromArgs(args)
    configurationProvider.initParameters(pt)
    Await.ready(subjectLibrary.initialize(), Duration(5, SECONDS))
  }

  def getStateBackend: StateBackend = {
    val sb = new FsStateBackend(getStateBackendPath, true)

    sb
  }

  /**
  * Creates a new stream stream execution environment with some default configuration
  * @return
  */

}
