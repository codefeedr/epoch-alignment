package org.codefeedr.core.library.internal.kafka.sink

import org.apache.flink.types.Row
import org.codefeedr.configuration.KafkaConfiguration
import org.codefeedr.core.library.SubjectFactoryComponent
import org.codefeedr.core.library.metastore.{JobNode, SubjectNode}
import org.codefeedr.model._
import org.codefeedr.util.EventTime

import scala.reflect.ClassTag

class GenericTrailedRecordSink[TElement: ClassTag: EventTime](
    subjectNode: SubjectNode,
    jobNode: JobNode,
    kafkaConfiguration: KafkaConfiguration,
    kafkaProducerFactory: KafkaProducerFactory,
    epochStateManager: EpochStateManager,
    override val sinkUuid: String)(subjectFactoryComponent: SubjectFactoryComponent)
    extends KafkaSink[TElement, Row, RecordSourceTrail](subjectNode,
                                                        jobNode,
                                                        kafkaConfiguration,
                                                        kafkaProducerFactory,
                                                        epochStateManager,
                                                        s"generic ${subjectNode.name}") {

  @transient private lazy val transformer =
    subjectFactoryComponent.subjectFactory.getTransformer[TElement](subjectType)

  override def transform(value: TElement): (RecordSourceTrail, Row) = {

    val trailed = transformer(value)
    (trailed.trail, trailed.row)
  }
}
