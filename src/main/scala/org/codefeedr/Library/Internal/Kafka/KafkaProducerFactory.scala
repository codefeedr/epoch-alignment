/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.codefeedr.Library.Internal.Kafka

import org.apache.kafka.clients.producer.KafkaProducer

/**
  * Created by Niels on 11/07/2017.
  */
object KafkaProducerFactory {

  /**
    * Create a kafka producer for a specific data and key type
    * @tparam TData Type of the data object
    * @tparam TKey Type of the key identifying the data object
    * @return A kafka producer capable of pushing the tuple to kafka
    */
  def create[TKey, TData]: KafkaProducer[TKey, TData] =
    new KafkaProducer[TKey, TData](KafkaConfig.properties,
                                   new KafkaSerializer[TKey],
                                   new KafkaSerializer[TData])
}
