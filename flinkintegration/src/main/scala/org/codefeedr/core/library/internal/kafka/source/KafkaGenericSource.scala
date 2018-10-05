package org.codefeedr.core.library.internal.kafka.source

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.codefeedr.configuration.KafkaConfiguration
import org.codefeedr.core.library.metastore.{JobNode, SubjectNode}

import scala.reflect._

/**
  * Simple generic kafka source
  * @param subjectNode subject node to write the data to
  * @param jobNode node of the jub the source belongs to
  * @param kafkaConsumerFactory factory to obtain kafka consumers
  * @param sourceUuid unique identifier of the source
  * @param ct classtag of the generic type
  * @tparam TElement entity type of the source
  */
class KafkaGenericSource[TElement](
    subjectNode: SubjectNode,
    jobNode: JobNode,
    kafkaConfiguration: KafkaConfiguration,
    kafkaConsumerFactory: KafkaConsumerFactory,
    override val sourceUuid: String)(implicit ct: ClassTag[TElement])
    extends KafkaSource[TElement, TElement, Object](subjectNode,
                                                    jobNode,
                                                    kafkaConfiguration,
                                                    kafkaConsumerFactory) {

  override def transform(value: TElement, key: Object): TElement = value

  /**
    * Get typeinformation of the returned type
    *
    * @return
    */
  override def getProducedType: TypeInformation[TElement] = {
    TypeInformation.of[TElement](ct.runtimeClass.asInstanceOf[Class[TElement]])
  }
}
