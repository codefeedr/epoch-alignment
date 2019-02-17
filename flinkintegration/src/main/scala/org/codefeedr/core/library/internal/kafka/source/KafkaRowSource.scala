/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.codefeedr.core.library.internal.kafka.source

import org.apache.flink.types.Row
import org.codefeedr.configuration.KafkaConfiguration
import org.codefeedr.core.library.TypeInformationServices
import org.codefeedr.core.library.metastore.{JobNode, SubjectNode}
import org.codefeedr.model.RecordSourceTrail
import org.codefeedr.util.NoEventTime._

/**
  * KafkaSource that exposes a subject as stream of table API rows
  * @param subjectNode
  */
class KafkaRowSource(subjectNode: SubjectNode,
                     jobNode: JobNode,
                     kafkaConfiguration: KafkaConfiguration,
                     kafkaConsumerFactory: KafkaConsumerFactory,
                     override val sourceUuid: String)
    extends KafkaSource[Row, Row, RecordSourceTrail](subjectNode,
                                                     jobNode,
                                                     kafkaConfiguration,
                                                     kafkaConsumerFactory,
                                                     s"rowSource ${subjectNode.name}") {

  /**
    * Get typeinformation of the returned type
    *
    * @return
    */
  override def getProducedType = TypeInformationServices.getEnrichedRowTypeInfo(subjectType)

  override def transform(value: Row, key: RecordSourceTrail): Row = value
}
