package org.codefeedr.configuration

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * This component handles the global configuration
  */

trait ConfigurationProdiverComponent {
  val configurationProvider: ConfigurationProvider


  class ConfigurationProviderImpl extends ConfigurationProvider with LazyLogging {
    @volatile private var requested = false
    @volatile private var _parameterTool: Option[ParameterTool] = None

    lazy val parameterTool: ParameterTool = getParameterTool

    /**
      * Sets the configuration
      *
      * @param configuration custom configuration to initialzie with
      */
    def initConfiguration(configuration: ParameterTool) {
      if (requested) {
        //Just to validate, the configuration is not modified after it has already been retrieved by some component
        throw new IllegalStateException("Cannot set parametertool after parametertool was already requested")
      }

      //Set the parameters in the stream execution environment
      try {
        val env = ExecutionEnvironment.getExecutionEnvironment
        env.getConfig.setGlobalJobParameters(configuration)
      } catch {
        case e: Exception => logger.warn(s"Cannot retrieve execution environment. Did you use this configuration provider outside a job? Error: ${e.getMessage}", e)
      }
      //Store parameter tool in the static context
      //Needed to also make the components work when there is no stream execution environment
      logger.debug("Setting parameter tool")
      _parameterTool = Some(configuration)

    }


    /**
      * Reads the parameterTool from the execution environment, or the explicitly initialized parametertool
      *
      * @return
      */
    private def getParameterTool: ParameterTool = {
      requested = true
      _parameterTool.getOrElse[ParameterTool]({
        logger.debug("Retrieving parameters from the execution environment")
        val env = ExecutionEnvironment.getExecutionEnvironment
        ParameterTool.fromMap(env.getConfig.getGlobalJobParameters.toMap)
      })
    }

    /**
      * Tries to key the value for the given key from the parametertool
      *
      * @param key key value to search for
      * @return value if the key exists in the parameter tool, Otherwise None
      */
    def tryGet(key: String): Option[String] = {
      if (parameterTool.has(key)) {
        Some(parameterTool.get(key))
      } else {
        None
      }
    }

  }
}

trait ConfigurationProvider {
  /**
    * Sets the configuration
    * @param configuration custom configuration to initialize with
    */
  def initConfiguration(configuration: ParameterTool): Unit

  /**
    * Retrieve the parameterTool for the global configuration
    * @return
    */
  def parameterTool: ParameterTool

  /**
    * Tries to key the value for the given key from the parametertool
    * @param key key value to search for
    * @return value if the key exists in the parameter tool, Otherwise None
    */
  def tryGet(key:String):Option[String]
}
