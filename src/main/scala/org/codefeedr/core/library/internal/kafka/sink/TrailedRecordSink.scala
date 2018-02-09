package org.codefeedr.core.library.internal.kafka.sink

import org.apache.kafka.clients.producer.ProducerRecord
import org.codefeedr.model.{SubjectType, TrailedRecord}

class TrailedRecordSink(override val subjectType: SubjectType, override val sinkUuid: String)
  extends KafkaSink[TrailedRecord] {
  override def invoke(trailedRecord: TrailedRecord): Unit = {
    logger.debug(s"Producer ${getLabel}sending a message to topic $topic")
    kafkaProducer.send(new ProducerRecord(topic, trailedRecord.trail, trailedRecord.row))
  }
}