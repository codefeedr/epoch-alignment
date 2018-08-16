package org.codefeedr.configuration

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * This class handles the configuration in static context
  * Configuration either is set explicitly, or is retrieved from the streamexecutionenvironment
  */
object ConfigurationProvider extends LazyLogging{
  @volatile private var requested = false
  @volatile private var _parameterTool: Option[ParameterTool] = None

  private lazy val parameterTool = getParameterTool()

  /**
    * Sets the configuration
    * @param configuration
    */
  def setConfiguration(configuration: ParameterTool) {
    if(requested) {
      //Just to validate, the configuration is not modified after it has already been retrieved by some component
      throw new IllegalStateException("Cannot set parametertool after parametertool was already requested")
    }

    //Set the parameters in the stream execution environment
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment
      env.getConfig.setGlobalJobParameters(configuration)
    } catch {
      case e:Exception => logger.warn(s"Cannot retrieve execution environment. Did you use this configuration provider outside a job? Error: ${e.getMessage}",e)
    }
    //Store parameter tool in the static context
    //Needed to also make the components work when there is no stream execution environment
    logger.debug("Setting parameter tool")
    _parameterTool = Some(configuration)

  }


  /**
    * Reads the parameterTool from the execution environment, or the explicitly initialized parametertool
    * @return
    */
  def getParameterTool(): ParameterTool = {
    requested = true
    _parameterTool.getOrElse[ParameterTool]({
      logger.debug("Retrieving parameters from the execution environment")
      val env = ExecutionEnvironment.getExecutionEnvironment
      ParameterTool.fromMap(env.getConfig.getGlobalJobParameters.toMap)
    })
  }

  def getKafkaConfiguration(): KafkaConfiguration = KafkaConfiguration(parameterTool)

}
