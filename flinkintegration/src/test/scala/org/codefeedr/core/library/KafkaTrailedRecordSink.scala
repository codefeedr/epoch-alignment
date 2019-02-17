package org.codefeedr.core.library

import org.apache.flink.types.Row
import org.codefeedr.configuration.KafkaConfiguration
import org.codefeedr.core.library.internal.kafka.sink.{EpochStateManager, KafkaProducerFactory, KafkaSink}
import org.codefeedr.core.library.metastore.{JobNode, SubjectNode}
import org.codefeedr.model.{RecordSourceTrail, TrailedRecord}
import org.codefeedr.util.EventTime

/**
  * Base class for all trailed record sources
  * @param subjectNode
  * @param jobNode
  * @param kafkaProducerFactory
  * @param epochStateManager
  * @tparam TSink
  */
abstract class KafkaTrailedRecordSink[TSink : EventTime](subjectNode: SubjectNode,
                                             jobNode: JobNode,
                                               kafkaConfiguration: KafkaConfiguration,
                                             kafkaProducerFactory: KafkaProducerFactory,
                                             epochStateManager: EpochStateManager,
                                                         run:String)
                                             extends KafkaSink[TSink,Row,RecordSourceTrail](
                                               subjectNode,
                                               jobNode,
                                               kafkaConfiguration,
                                               kafkaProducerFactory,
                                               epochStateManager,
                                               run) {


}
