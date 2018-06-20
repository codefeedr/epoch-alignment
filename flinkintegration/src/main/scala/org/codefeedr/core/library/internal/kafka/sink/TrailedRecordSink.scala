package org.codefeedr.core.library.internal.kafka.sink

import org.apache.flink.types.Row
import org.codefeedr.core.library.LibraryServices
import org.codefeedr.core.library.metastore.{JobNode, SubjectNode}
import org.codefeedr.model.{RecordSourceTrail, SubjectType, TrailedRecord}

class TrailedRecordSink(subjectNode: SubjectNode,
                        jobNode: JobNode,
                        kafkaProducerFactory: KafkaProducerFactory,
                        epochStateManager: EpochStateManager,
                        override val sinkUuid: String)
    extends KafkaSink[TrailedRecord,Row,RecordSourceTrail](subjectNode, jobNode, kafkaProducerFactory, epochStateManager) {

    override def transform(value: TrailedRecord): (RecordSourceTrail, Row) = (value.trail, value.row)
}
