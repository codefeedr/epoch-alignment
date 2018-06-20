package org.codefeedr.core.library

import org.apache.flink.types.Row
import org.codefeedr.core.library.internal.kafka.sink.{EpochStateManager, KafkaProducerFactory, KafkaSink}
import org.codefeedr.core.library.metastore.{JobNode, SubjectNode}
import org.codefeedr.model.{RecordSourceTrail, TrailedRecord}

/**
  * Base class for all trailed record sources
  * @param subjectNode
  * @param jobNode
  * @param kafkaProducerFactory
  * @param epochStateManager
  * @tparam TSink
  */
abstract class KafkaTrailedRecordSink[TSink](subjectNode: SubjectNode,
                                             jobNode: JobNode,
                                             kafkaProducerFactory: KafkaProducerFactory,
                                             epochStateManager: EpochStateManager)
                                             extends KafkaSink[TSink,TrailedRecord, Row](
                                               subjectNode,
                                               jobNode,
                                               kafkaProducerFactory,
                                               epochStateManager) {


}
