package org.codefeedr.experiments

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.codefeedr.core.library.CodefeedrComponents
import org.apache.flink.runtime.state.filesystem.FsStateBackend

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}

trait ExperimentBase extends CodefeedrComponents with LazyLogging {

  protected def getStateBackendPath: String = configurationProvider.get("statebackend.path")

  protected def getParallelism: Int = 2
  protected def getKafkaParallelism: Int = 2
  protected def getRun = configurationProvider.get("run")

  @transient protected lazy val awaitDuration: Duration =
    configurationProvider.getDefaultAwaitDuration

  @transient protected lazy val getEnvironment: StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    configurationProvider.initEc(env.getConfig)
    //env.getConfig.disableGenericTypes()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE)
    env.setStateBackend(getStateBackend)
    env.setRestartStrategy(
      RestartStrategies.fixedDelayRestart(
        0,
        Time.seconds(60)
      ))
    env.setParallelism(getParallelism)
    env
  }

  def execute(jobName: String) = {
    getEnvironment.execute(jobName)
    logger.info("Job finished, closing zookeeper")
    zkClient.close()
  }

  def initialize(args: Array[String]) = {
    val pt = ParameterTool.fromArgs(args)
    configurationProvider.initParameters(pt)
    awaitReady(subjectLibrary.initialize())
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
