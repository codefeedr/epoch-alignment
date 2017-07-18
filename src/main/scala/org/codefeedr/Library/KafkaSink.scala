package org.codefeedr.Library

import java.util.UUID

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.kafka.clients.producer.ProducerRecord
import org.codefeedr.Library.Internal.{KafkaController, KafkaProducerFactory, SubjectTypeFactory}
import org.codefeedr.Model.RecordIdentifier

import scala.reflect.runtime.{universe => ru}

/**
  * Created by Niels on 11/07/2017.
  */
class KafkaSink[TData: ru.TypeTag] extends RichSinkFunction[TData] {
  @transient private lazy val kafkaProducer = KafkaProducerFactory.create[RecordIdentifier, TData]

  //Unique identifier for the sink
  @transient private lazy val uuid = UUID.randomUUID().toString

  @transient private lazy val subjectType = SubjectTypeFactory.getSubjectType[TData]

  @transient private lazy val topic = subjectType.name

  @transient private var Sequence: Long = 0

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
    KafkaController.GuaranteeTopic(topic)
    super.open(parameters)
  }

  override def invoke(value: TData): Unit = {
    //Validate if this actually performs
    kafkaProducer.send(new ProducerRecord(topic, getNextId, value))
  }

}
