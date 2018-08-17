package org.codefeedr.configuration
import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.java.utils.ParameterTool

import scala.collection.immutable
import scala.collection.JavaConverters._



abstract class ConfigurationMapping(val mapping: String => AnyRef, val default: Option[AnyRef])

case class CM[TConfig<:AnyRef](m: String => TConfig, d: Option[TConfig]) extends ConfigurationMapping(m,d)

trait KafkaConfigurationComponent {
  this:ConfigurationProdiverComponent =>

  val kafkaConfiguration:KafkaConfiguration

  class KafkaConfigurationImpl extends KafkaConfiguration with LazyLogging {
    case class KafkaConfigurationStore(kafkaConfig: immutable.Map[String, AnyRef], defaultPartitions: Int)
    //Prefix for keys, added to support multimple kafka configurations in the future (if needed)
    val prefix = ""

    private val mapping = immutable.Map[String, ConfigurationMapping](
      " bootstrap.servers"  -> CM(v => v,None),
      "retries"                     -> CM[Integer](v => v.toInt,Some(1)),
      "auto.offset.reset"           -> CM(v => v,Some("earliest")),
      "auto.commit.interval.ms"     -> CM[Integer](v => v.toInt,Some(100))
    )

    private lazy val config:KafkaConfigurationStore =  {
      val values = mapping
        .map(kvp => {
          //Prefix is added in front of the keys that are read from the parameter array
          val prefixedKey = s"$prefix${kvp._1}"
          val value = configurationProvider.tryGet(prefixedKey) match {
            case None => kvp._2.default.getOrElse(() =>  throw new IllegalStateException(s"No value found for required configuration: ${kvp._1}"))
            case Some(v) => v
          }
          (kvp._1,value)
        })

      val partitions = configurationProvider.tryGet( "partitions").getOrElse("4").toInt

      val parsed = KafkaConfigurationStore(values,partitions)
      logger.debug(s"Parsed kafka configuration to $parsed")
      parsed
    }


    /**
      * Retrieve the properties represented by this case class
      */
    def getProperties:Properties = {
      val p = new Properties()
      p.putAll(config.kafkaConfig.asJava)
      p
    }

    def defaultPartitions: Int = config.defaultPartitions

  }
}

trait KafkaConfiguration {
  /**
    * Retrieve the configuration as java Properties (so it can be passed to the kafka driver)
    * @return the properties object
    */
  def getProperties:Properties

  /**
    * @return The amount of partitions that should be used as default when creating a kafka topic
    */
  def defaultPartitions:Int
}





