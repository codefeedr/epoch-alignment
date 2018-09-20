package org.codefeedr.core.library.internal.kafka.sink

import org.apache.flink.types.Row
import org.codefeedr.core.library.SubjectFactoryComponent
import org.codefeedr.core.library.metastore.{JobNode, SubjectNode}
import org.codefeedr.model._
import org.codefeedr.util.EventTime

import scala.reflect.ClassTag

class GenericTrailedRecordSink[TElement: ClassTag](
    subjectNode: SubjectNode,
    jobNode: JobNode,
    kafkaProducerFactory: KafkaProducerFactory,
    epochStateManager: EpochStateManager,
    override val sinkUuid: String)(subjectFactoryComponent: SubjectFactoryComponent)
    extends KafkaSink[TElement, Row, RecordSourceTrail](subjectNode,
                                                        jobNode,
                                                        kafkaProducerFactory,
                                                        epochStateManager) {

  @transient private lazy val transformer =
    subjectFactoryComponent.subjectFactory.getTransformer[TElement](subjectType)

  override def transform(value: TElement): (RecordSourceTrail, Row) = {

    val trailed = transformer(value)
    (trailed.trail, trailed.row)
  }
}
