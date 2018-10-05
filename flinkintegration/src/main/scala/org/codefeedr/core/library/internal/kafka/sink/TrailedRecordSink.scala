package org.codefeedr.core.library.internal.kafka.sink

import org.apache.flink.types.Row
import org.codefeedr.configuration.KafkaConfiguration
import org.codefeedr.core.library.metastore.{JobNode, SubjectNode}
import org.codefeedr.model.{RecordSourceTrail, TrailedRecord}
import org.codefeedr.util.NoEventTime._

/**
  * A sink for directly dumping trailed records
  * @param subjectNode node of the subject represented by the trailed record
  * @param jobNode node of the job this sink is part of
  * @param kafkaProducerFactory factory for kafka producers
  * @param epochStateManager
  * @param sinkUuid
  */
class TrailedRecordSink(subjectNode: SubjectNode,
                        jobNode: JobNode,
                        kafkaConfiguration: KafkaConfiguration,
                        kafkaProducerFactory: KafkaProducerFactory,
                        epochStateManager: EpochStateManager,
                        override val sinkUuid: String)
    extends KafkaSink[TrailedRecord, Row, RecordSourceTrail](subjectNode,
                                                             jobNode,
                                                             kafkaConfiguration,
                                                             kafkaProducerFactory,
                                                             epochStateManager) {

  override def transform(value: TrailedRecord): (RecordSourceTrail, Row) = {
    (value.trail, value.row)
  }
}
