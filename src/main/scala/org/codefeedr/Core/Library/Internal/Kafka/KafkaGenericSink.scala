

package org.codefeedr.Core.Library.Internal.Kafka

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.kafka.clients.producer.ProducerRecord
import org.codefeedr.Core.Library.{SubjectFactory, SubjectLibrary}
import org.codefeedr.Model.{ActionType, Record, RecordSourceTrail, SubjectType}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

/**
  * Created by Niels on 31/07/2017.
  */
class KafkaGenericSink[TData: ru.TypeTag: ClassTag](subjectType: SubjectType)
    extends RichSinkFunction[TData]
    with LazyLogging {
  @transient private lazy val kafkaProducer = {
    val producer = KafkaProducerFactory.create[RecordSourceTrail, Record]
    logger.debug(s"Producer $uuid created for topic $topic")
    producer
  }

  @transient private lazy val topic = s"${subjectType.name}_${subjectType.uuid}"

  //A random identifier for this specific sink
  @transient private[Kafka] lazy val uuid = UUID.randomUUID()

  @transient private lazy val Transformer = SubjectFactory.GetTransformer[TData](subjectType)

  override def close(): Unit = {
    logger.debug(s"Closing producer $uuid")
    kafkaProducer.close()
    Await.ready(SubjectLibrary.UnRegisterSink(subjectType.name, uuid.toString), Duration.Inf)
  }

  override def open(parameters: Configuration): Unit = {
    logger.debug(s"Opening producer $uuid")
    Await.ready(SubjectLibrary.RegisterSink(subjectType.name, uuid.toString), Duration.Inf)
    super.open(parameters)
  }

  override def invoke(value: TData): Unit = {
    val data = Transformer.apply(value)
    kafkaProducer.send(new ProducerRecord(topic, data.trail, data.record))
  }
}
