package org.codefeedr.configuration
import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.model.SubjectType

import scala.collection.immutable
import scala.collection.JavaConverters._

abstract class ConfigurationMapping(val mapping: String => AnyRef, val default: Option[AnyRef])

case class CM[TConfig <: AnyRef](m: String => TConfig, d: Option[TConfig])
    extends ConfigurationMapping(m, d)

trait KafkaConfigurationComponent extends Serializable { this: ConfigurationProviderComponent =>



  val kafkaConfiguration: KafkaConfiguration

  class KafkaConfigurationImpl extends KafkaConfiguration with LazyLogging {
    case class KafkaConfigurationStore(kafkaConfig: immutable.Map[String, AnyRef],
                                       defaultPartitions: Int)
    //Prefix for keys, added to support multimple kafka configurations in the future (if needed)
    val prefix = "kafka."

    //These mappings are not serializable
    @transient private lazy val adminMapping = immutable.Map[String, ConfigurationMapping](
      "bootstrap.servers" -> CM(v => v, None),
      "retries" -> CM[Integer](v => v.toInt, Some(1))
    )
    @transient private lazy val consumerMapping = immutable.Map[String, ConfigurationMapping](
      //  "auto.commit.enable" -> CM(v => v, Some("true")),
      "bootstrap.servers" -> CM(v => v, None),
      "auto.offset.reset" -> CM(v => v, Some("earliest")),
      "auto.commit.interval.ms" -> CM[Integer](v => v.toInt, Some(100))
    )
    @transient private lazy val producerMapping = adminMapping

    /**
      * Admin properties for kafka
      */
    def getAdminProperties: Properties = toKafkaConfiguration(adminMapping)

    def getConsumerProperties: Properties = toKafkaConfiguration(consumerMapping)

    def getProducerProperties: Properties = toKafkaConfiguration(producerMapping)

    def defaultPartitions: Int = configurationProvider.tryGet("partitions").getOrElse("4").toInt

    override def getTopic(subjectName: String, subjectUuid: String): String =
      s"codefeedr_${subjectName}_$subjectUuid"

    /**
      * Uses the mapping and the
      * @param mapping
      * @return
      */
    private def toKafkaConfiguration(mapping: Map[String, ConfigurationMapping]): Properties = {
      val map = mapping
        .map(kvp => {
          //Prefix is added in front of the keys that are read from the parameter array
          val prefixedKey = s"$prefix${kvp._1}"
          val value = configurationProvider.tryGet(prefixedKey) match {
            case None =>
              kvp._2.default.getOrElse(
                () =>
                  throw new IllegalStateException(
                    s"No value found for required configuration: ${kvp._1}"))
            case Some(v) => v
          }
          (kvp._1, value)
        })
      val properties = new Properties()
      properties.putAll(map.asJava)
      properties
    }
  }
}

trait KafkaConfiguration extends Serializable {

  /**
    * Retrieve the configuration as java Properties (so it can be passed to the kafka driver)
    * @return the properties object
    */
  def getAdminProperties: Properties

  /**
    * Retrieve configuration for the kafka consumer
    * @return
    */
  def getConsumerProperties: Properties

  /**
    *
    * @return
    */
  def getProducerProperties: Properties

  /**
    * @return The amount of partitions that should be used as default when creating a kafka topic
    */
  def defaultPartitions: Int

  /**
    * Uses subjectuuid and subjectname to generate a name for the kafa subject
    * @param subjectName name of the codefeedr subject
    * @param subjectUuid unique identifier generated for the subject
    * @return
    */
  def getTopic(subjectName: String, subjectUuid: String): String

  /**
    * Retrieve the topic name in kafka based on subjectType
    * @param subjectType type to retrieve the topic for
    * @return
    */
  def getTopic(subjectType: SubjectType): String = getTopic(subjectType.name, subjectType.uuid)
}
