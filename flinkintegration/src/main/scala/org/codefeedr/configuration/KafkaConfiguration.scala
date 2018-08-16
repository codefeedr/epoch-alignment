package org.codefeedr.configuration
import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.java.utils.ParameterTool

import scala.collection.immutable
import scala.collection.JavaConverters._



abstract class ConfigurationMapping(val mapping: String => AnyRef, val default: Option[AnyRef])

case class CM[TConfig<:AnyRef](m: String => TConfig, d: Option[TConfig]) extends ConfigurationMapping(m,d)

case class KafkaConfiguration(config: immutable.Map[String, AnyRef]) {
  /**
    * Retrieve the properties represented by this
    */
  lazy val getProperties:Properties = {
    val p = new Properties()
    p.putAll(config.asJava)
    p
  }
}



object KafkaConfiguration extends LazyLogging {
  private val mapping = immutable.Map[String, ConfigurationMapping](
    " bootstrap.servers"  -> CM(v => v,None),
    "retries"                     -> CM(v => v.toInt,Some(1)),
    "auto.offset.reset"           -> CM(v => v,Some("earliest")),
    "auto.commit.interval.ms"     -> CM(v => v.toInt,Some(100))
  )


  /**
    * Tries to key the value for the given key from the parametertool
    * @param pt parameter tool to search for key
    * @param key key value to search for
    * @return value if the key exists in the parameter tool, Otherwise None
    */
  def tryGet(pt:ParameterTool, key: String):Option[String] = {
    if(pt.has(key)) {
      Some(pt.get(key))
    } else {
      None
    }
  }


  /**
    * Construct kafka configuration from parameters on the url
    * @param params url parameters
    */
  def Apply(params: Array[String], prefix:String = ""):KafkaConfiguration = {
    val pt = ParameterTool.fromArgs(params)
    val values = mapping
      .map(kvp => {
        //Prefix is added in front of the keys that are read from the parameter array
        val prefixedKey = s"$prefix${kvp._1}"
        val value = tryGet(pt,prefixedKey) match {
          case None => kvp._2.default.getOrElse(_ =>  throw new IllegalStateException(s"No value found for required configuration: ${kvp._1}"))
          case Some(v) => v
        }
        (kvp._1,value)
    })

    val parsed = KafkaConfiguration(values)
    logger.debug(s"Parsed kafka configuration to $parsed")
    parsed
  }
}
