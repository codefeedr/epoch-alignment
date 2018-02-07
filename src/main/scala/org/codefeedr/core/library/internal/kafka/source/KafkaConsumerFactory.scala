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

package org.codefeedr.core.Library.Internal.Kafka.Source

import java.util.Properties

import org.apache.kafka.clients.consumer._
import org.codefeedr.core.Library.Internal.Kafka.{KafkaConfig, KafkaDeserialiser}

import scala.reflect.ClassTag

/**
  * Created by Niels on 14/07/2017.
  */
object KafkaConsumerFactory {

  /**
    * Create a new kafka consumer
    * @param group Groupname. Each group recieves each value once
    * @tparam TKey Type of the key used by kafka
    * @tparam TData Data type used to send to kafka
    * @return
    */
  def create[TKey: ClassTag, TData: ClassTag](group: String): KafkaConsumer[TKey, TData] = {
    //Kafka consumer constructor is not thread safe!
    val properties = KafkaConfig.consumerPropertes.clone().asInstanceOf[Properties]
    properties.setProperty("group.id", group)
    //properties.setProperty("enable.auto.commit", "false") //Disable auto commit because we use manual commit
    new KafkaConsumer[TKey, TData](properties,
                                   new KafkaDeserialiser[TKey],
                                   new KafkaDeserialiser[TData])

  }
}
