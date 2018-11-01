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

import java.util.{Properties, UUID}

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer._
import org.codefeedr.configuration.{ConfigurationProviderComponent, KafkaConfigurationComponent}
import org.codefeedr.core.library.internal.kafka.KafkaDeserialiser

import scala.reflect.ClassTag

trait KafkaConsumerFactoryComponent {
  this: KafkaConfigurationComponent with ConfigurationProviderComponent =>
  val kafkaConsumerFactory: KafkaConsumerFactory

  /**
    * Created by Niels on 14/07/2017.
    */
  class KafkaConsumerFactoryImpl extends Serializable with LazyLogging with KafkaConsumerFactory {

    /**
      * Create a new kafka consumer
      * @param group Groupname. Each group receives each value once
      * @tparam TKey Type of the key used by kafka
      * @tparam TData Data type used to send to kafka
      * @return
      */
    override def create[TKey: ClassTag, TData: ClassTag](
        group: String): KafkaConsumer[TKey, TData] = {
      //Kafka consumer constructor is not thread safe!
      val properties = kafkaConfiguration.getConsumerProperties

      properties.setProperty("group.id", group)
      //Only read committed records
      //properties.setProperty("isolation.level", "read_committed")
      logger.info(s"Creating consumer in group $group")
      properties.setProperty("enable.auto.commit", "false") //Disable auto commit because we use manual commit
      new KafkaConsumer[TKey, TData](properties,
                                     new KafkaDeserialiser[TKey],
                                     new KafkaDeserialiser[TData])

    }
  }
}

trait KafkaConsumerFactory {

  /**
    * Create a new kafka consumer
    *
    * @param group Groupname. Each group receives each value once
    * @tparam TKey  Type of the key used by kafka
    * @tparam TData Data type used to send to kafka
    * @return
    */
  def create[TKey: ClassTag, TData: ClassTag](group: String): KafkaConsumer[TKey, TData]
}
