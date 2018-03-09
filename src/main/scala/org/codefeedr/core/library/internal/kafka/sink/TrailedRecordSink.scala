package org.codefeedr.core.library.internal.kafka.sink

import org.apache.flink.types.Row
import org.codefeedr.model.{RecordSourceTrail, SubjectType, TrailedRecord}

class TrailedRecordSink(override var subjectType: SubjectType, override val sinkUuid: String)
    extends KafkaSink[TrailedRecord] {

  override def transform(value: TrailedRecord): (RecordSourceTrail, Row) = (value.trail, value.row)
}
