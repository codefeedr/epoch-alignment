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

package org.codefeedr.core.library.internal.kafka.sink

import org.apache.flink.types.Row
import org.apache.kafka.clients.producer.ProducerRecord
import org.codefeedr.core.library.{LibraryServices, SubjectFactoryComponent}
import org.codefeedr.core.library.metastore.{JobNode, SubjectNode}
import org.codefeedr.model.{RecordSourceTrail, SubjectType, TrailedRecord}

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

/**
  * Created by Niels on 31/07/2017.
  */
class KafkaGenericSink[TData: ru.TypeTag: ClassTag](val subjectNode: SubjectNode,
                                                    jobNode: JobNode,
                                                    kafkaProducerFactory: KafkaProducerFactory,
                                                    epochStateManager: EpochStateManager,
                                                    override val sinkUuid: String
) extends KafkaSink[TData,TData,Object](subjectNode, jobNode, kafkaProducerFactory, epochStateManager) {

  override def transform(value: TData): (Object,TData) = (null,value)
}
