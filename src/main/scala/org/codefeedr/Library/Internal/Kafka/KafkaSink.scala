package org.codefeedr.Library.Internal.Kafka

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.kafka.clients.producer.ProducerRecord
import org.codefeedr.Model.{ActionType, Record, RecordIdentifier, SubjectType}

import scala.reflect.runtime.{universe => ru}

/**
  * A simple kafka sink, pushing all records as "new" data
  * Created by Niels on 11/07/2017.
  */
class KafkaSink[TData: ru.TypeTag](subjectType: SubjectType)
    extends RichSinkFunction[TData]
    with LazyLogging {
  @transient private lazy val kafkaProducer = {
    val producer = KafkaProducerFactory.create[RecordIdentifier, Record]
    logger.debug(s"Producer $uuid created for topic $topic")
    producer
  }
  @transient private lazy val topic = s"${subjectType.name}_${subjectType.uuid}"
  @transient private var Sequence: Long = 0

  //A random identifier for this specific sink, making the keys pushing to kafka unique
  @transient private val uuid = UUID.randomUUID().toString

  /**
    * @return Incremental unique identifier for the record
    */
  private def getNextId: RecordIdentifier = {
    val id = RecordIdentifier(Sequence, uuid)
    Sequence += 1
    id
  }

  override def close(): Unit = {
    kafkaProducer.close()
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def invoke(value: TData): Unit = {
    //Validate if this actually performs
    kafkaProducer.send(new ProducerRecord(topic, getNextId, Record(value, ActionType.Add)))
  }

}
