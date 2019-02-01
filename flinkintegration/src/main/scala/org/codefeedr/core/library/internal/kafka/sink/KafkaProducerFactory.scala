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

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.codefeedr.configuration.{ConfigurationProviderComponent, KafkaConfigurationComponent}
import org.codefeedr.core.library.internal.kafka.KafkaSerialiser

import scala.concurrent._
import scala.reflect.ClassTag

trait KafkaProducerFactoryComponent {
  this: KafkaConfigurationComponent with ConfigurationProviderComponent =>
  val kafkaProducerFactory: KafkaProducerFactory

  /**
    * Created by Niels on 11/07/2017.
    */
  class KafkaProducerFactoryImpl extends LazyLogging with KafkaProducerFactory with Serializable {
    def create[TKey: ClassTag, TData: ClassTag](
        transactionalId: String,
        enableTransactions: Boolean): KafkaProducer[TKey, TData] = {
      val properties = kafkaConfiguration.getProducerProperties

      if (enableTransactions) {
        logger.info(s"Creating producer with id $transactionalId")
        properties.setProperty("transactional.id", transactionalId)
      } else {
        logger.info(s"Creating producer without transactions: $transactionalId")
      }
      //properties.setProperty("enable.idempotence", "true")
      val producer = new KafkaProducer[TKey, TData](properties,
                                                    new KafkaSerialiser[TKey],
                                                    new KafkaSerialiser[TData])
      if (enableTransactions) {
        producer.initTransactions()
      }
      producer.flush()
      producer
    }
  }

}

trait KafkaProducerFactory extends Serializable {

  /**
    * Create a kafka producer for a specific data and key type
    * The kafka producer will be initialized for transactions
    *
    * @tparam TData Type of the data object
    * @tparam TKey  Type of the key identifying the data object
    * @return A kafka producer capable of pushing the tuple to kafka
    */
  def create[TKey: ClassTag, TData: ClassTag](
      transactionalId: String,
      enableTransactions: Boolean): KafkaProducer[TKey, TData]
}
