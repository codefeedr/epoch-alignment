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

package org.codefeedr.core.library

import org.codefeedr.configuration._
import org.codefeedr.core.engine.query.StreamComposerFactoryComponent
import org.codefeedr.core.library.internal.kafka.KafkaControllerComponent
import org.codefeedr.core.library.internal.kafka.sink.{EpochStateManager, EpochStateManagerComponent, KafkaProducerFactoryComponent, KafkaTableSinkFactoryComponent}
import org.codefeedr.core.library.internal.kafka.source.KafkaConsumerFactoryComponent
import org.codefeedr.core.library.internal.zookeeper.{ZkClient, ZkClientComponent}
import org.codefeedr.core.library.metastore.SubjectLibraryComponent


/**
  * Bundle different configuration components
  * These components are not guaranteed to be singleton, due to Flink serializing and deserializing them
  */
trait ConfigurationModule extends Serializable
    with FlinkConfigurationProviderComponent
    with KafkaConfigurationComponent
    with ZookeeperConfigurationComponent
{
  lazy override val kafkaConfiguration: KafkaConfiguration = new KafkaConfigurationImpl()
}

//Serializable components that can be used inside flink operators
//These components are not guaranteed to be singletons!
trait ConfigurationComponents
  extends ConfigurationModule
  with KafkaConsumerFactoryComponent
  with KafkaProducerFactoryComponent {
  @transient lazy override val kafkaConsumerFactory = new KafkaConsumerFactoryImpl()
  @transient lazy override val kafkaProducerFactory = new KafkaProducerFactoryImpl()
}


/**
  * General components performing all the application logic
  */
trait CodefeedrComponents extends Serializable
    with ConfigurationComponents
    with ZkClientComponent
    with SubjectLibraryComponent
    with KafkaControllerComponent

    with KafkaConsumerFactoryComponent
    with KafkaProducerFactoryComponent

    with SubjectFactoryComponent
    with StreamComposerFactoryComponent
    with EpochStateManagerComponent
    with KafkaTableSinkFactoryComponent {

  @transient lazy override implicit val zkClient:ZkClient = new ZkClientImpl()
  @transient lazy override val subjectLibrary:SubjectLibrary = new SubjectLibrary()

  @transient lazy override val kafkaTableSinkFactory = new KafkaTableSinkFactoryImpl();
  @transient lazy override val subjectFactory = new SubjectFactoryController()
  @transient lazy override val streamComposerFactory = new StreamComposerFactory()
  @transient lazy override val epochStateManager = new EpochStateManager()
  @transient lazy override val kafkaController = new KafkaController()

}