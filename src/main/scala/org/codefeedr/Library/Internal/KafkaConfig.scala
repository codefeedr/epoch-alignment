package org.codefeedr.Library.Internal

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by Niels on 11/07/2017.
  */
object KafkaConfig {
  @transient lazy val conf: Config = ConfigFactory.load

  /**
    * Map configuration to java properties
    * Source: https://stackoverflow.com/questions/34337782/converting-typesafe-config-type-to-java-util-properties
    * @param config the config object to convert to properties
    * @return java properties object
    */
  def propsFromConfig(config: Config): Properties = {
    import scala.collection.JavaConversions._

    val props = new Properties()

    val map: Map[String, Object] = config
      .entrySet()
      .map({ entry =>
        entry.getKey -> entry.getValue.unwrapped()
      })(collection.breakOut)

    props.putAll(map)
    props
  }

  /**
    * Get the kafka configuration
    */
  @transient lazy val properties: Properties = propsFromConfig(
    conf.getConfig("codefeedr.kafka.server"))

  @transient lazy val consumerPropertes: Properties = propsFromConfig(
    conf.getConfig("codefeedr.kafka.consumer"))
}
