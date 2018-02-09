package org.codefeedr.core.library.internal.kafka.sink

import java.lang
import java.util.UUID

import org.apache.flink.api.java.tuple
import org.apache.flink.types.Row
import org.apache.kafka.clients.producer.ProducerRecord
import org.codefeedr.core.library.internal.KeyFactory
import org.codefeedr.model.{ActionType, Record, SubjectType, TrailedRecord}


class RowSink(override val subjectType: SubjectType, override val sinkUuid: String)
  extends KafkaSink[tuple.Tuple2[lang.Boolean, Row]] {
  @transient lazy val keyFactory = new KeyFactory(subjectType, UUID.randomUUID())

  override def invoke(value: tuple.Tuple2[lang.Boolean, Row]): Unit = {
    logger.debug(s"Producer ${GetLabel} sending a message to topic $topic")
    val actionType = if (value.f0) ActionType.Add else ActionType.Remove
    //TODO: Optimize these steps
    val record = Record(value.f1, subjectType.uuid, actionType)
    val trailed = TrailedRecord(record, keyFactory.getKey(record))
    kafkaProducer.send(new ProducerRecord(topic, trailed.trail, trailed.row))
  }
}
