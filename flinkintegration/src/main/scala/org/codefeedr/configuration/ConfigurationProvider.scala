package org.codefeedr.configuration


import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.codefeedr.util.OptionExtensions.DebuggableOption

import scala.collection.JavaConverters._

/**
  * This component handles the global configuration
  */


trait ConfigurationProvider extends Serializable {
  /**
    * Sets the configuration
    *
    * @param configuration custom configuration to initialize with
    */
  def initConfiguration(configuration: ParameterTool, propertiesFile:Option[String] = None): Unit

  /**
    * Retrieve the parameterTool for the global configuration
    *
    * @return
    */
  def parameterTool: ParameterTool

  /**
    * Tries to key the value for the given key from the parametertool
    *
    * @param key key value to search for
    * @return value if the key exists in the parameter tool, Otherwise None
    */
  def tryGet(key: String): Option[String]

  /**
    * Returns the value of the given key
    * If no default value is passed and the key does not exist, an exception is thrown
    *
    * @param key     key to look for in the configuration
    * @param default default value
    * @return
    */
  def get(key: String, default: Option[String] = None): String

  /**
    * Returns the value of the given key
    * If no default value is passed and the key does not exist, an exception is thrown
    *
    * @param key     key to look for in the configuration
    * @param default default value
    * @return
    */
  def getInt(key: String, default: Option[Int] = None): Int =
    get(key, default.map(o => o.toString)).toInt
}

trait ConfigurationProviderComponent {
  val configurationProvider: ConfigurationProvider


}

trait FlinkConfigurationProviderComponent extends ConfigurationProviderComponent {
  val configurationProvider:ConfigurationProvider
  class ConfigurationProviderImpl
    extends ConfigurationProvider
    with LazyLogging
  {
    //All these variables are transient, because the configuration provider should
    // re-initialize after deserialization, and use the configuration from the execution environment
    @transient @volatile private var requested = false
    @volatile private var _parameterTool: Option[ParameterTool] = None
    @transient lazy val parameterTool: ParameterTool = getParameterTool



    /**
      * Sets the configuration
      *
      * @param configuration custom configuration to initialize with
      * @param propertiesFile name of properties file to additionally initialize with
      */
    def initConfiguration(configuration: ParameterTool, propertiesFile:Option[String] = None):Unit = {
      if (requested) {
        //Just to validate, the configuration is not modified after it has already been retrieved by some component
        throw new IllegalStateException("Cannot set parametertool after parametertool was already requested")
      }
      //Store parameter tool in the static context
      //Needed to also make the components work when there is no stream execution environment

      logger.debug("Initialized parameter tool")
      _parameterTool = propertiesFile.flatMap(loadPropertiesFile) match {
        case Some(p) => Some(p.mergeWith(configuration))
        case None => Some(configuration)
      }
    }
    /**
      * Attempts to load the codefeedr.properties file
      * @param fileName Name of the properties file to load
      * @return A paremetertool if the file was found, none otherwise
      */
    private def loadPropertiesFile(fileName:String): Option[ParameterTool] =
      Option(getClass.getResourceAsStream(fileName))
        .info(s"Loading $fileName file", s"No $fileName resource file found.")
        .map(ParameterTool.fromPropertiesFile)



    /**
      * Reads the parameterTool from the codefeedr.properties file, execution environment, or the explicitly initialized parametertool
      * Makes sure that if the properties file was used, the properties are also registered as jobParameters
      *
      * @return
      */
    private def getParameterTool: ParameterTool =
      _parameterTool match {
        case Some(p) => p
        case None => throw new IllegalArgumentException("Cannot retrieve parametertool before calling initialize. Make sure to call initConfiguration as soon as possible in your application, and make sure this ConfigurationProviderComponent gets serialized with the Flink job")
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

    /**
      * @param key     key to look for in the configuration
      * @param default default value
      * @return
      */
    override def get(key: String, default: Option[String]): String = {
      tryGet(key) match {
        case None => default match {
          case None => throw new IllegalArgumentException(s"Cannot find a configuration for $key, and no default value was passed")
          case Some(v) => v
        }
        case Some(v) => v
      }
    }
  }
}


