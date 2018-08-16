package org.codefeedr.configuration
import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.java.utils.ParameterTool

import scala.collection.immutable
import scala.collection.JavaConverters._



abstract class ConfigurationMapping(val mapping: String => AnyRef, val default: Option[AnyRef])

case class CM[TConfig<:AnyRef](m: String => TConfig, d: Option[TConfig]) extends ConfigurationMapping(m,d)

trait KafkaConfigurationComponent {
  val kafkaConfiguration:KafkaConfiguration
}

case class KafkaConfiguration(kafkaConfig: immutable.Map[String, AnyRef], defaultPartitions: Int) {

  /**
    * Retrieve the properties represented by this case class
    */
  def getProperties:Properties = {
    val p = new Properties()
    p.putAll(kafkaConfig.asJava)
    p
  }
}



object KafkaConfiguration extends LazyLogging {
  private val mapping = immutable.Map[String, ConfigurationMapping](
    " bootstrap.servers"  -> CM(v => v,None),
    "retries"                     -> CM[Integer](v => v.toInt,Some(1)),
    "auto.offset.reset"           -> CM(v => v,Some("earliest")),
    "auto.commit.interval.ms"     -> CM[Integer](v => v.toInt,Some(100))
  )




  /**
    * Construct kafka configuration from parameters on the url
    * @param pt the parameterTool
    * @param prefix prefix of the configuration
    */
  def apply(pt:ParameterTool, prefix:String = ""): KafkaConfiguration = {
    val values = mapping
      .map(kvp => {
        //Prefix is added in front of the keys that are read from the parameter array
        val prefixedKey = s"$prefix${kvp._1}"
        val value = ConfigUtil.tryGet(pt,prefixedKey) match {
          case None => kvp._2.default.getOrElse(() =>  throw new IllegalStateException(s"No value found for required configuration: ${kvp._1}"))
          case Some(v) => v
        }
        (kvp._1,value)
    })

    val partitions = ConfigUtil.tryGet(pt, "partitions").getOrElse("4").toInt

    val parsed = KafkaConfiguration(values,partitions)
    logger.debug(s"Parsed kafka configuration to $parsed")
    parsed
  }
}
