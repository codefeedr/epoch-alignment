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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.codefeedr.configuration._
import org.codefeedr.core.engine.query.StreamComposerFactoryComponent
import org.codefeedr.core.library.internal.kafka.KafkaControllerComponent
import org.codefeedr.core.library.internal.kafka.sink.{
  EpochStateManager,
  EpochStateManagerComponent,
  KafkaProducerFactoryComponent,
  KafkaTableSinkFactoryComponent
}
import org.codefeedr.core.library.internal.kafka.source.KafkaConsumerFactoryComponent
import org.codefeedr.core.library.internal.zookeeper.{ZkClient, ZkClientComponent}
import org.codefeedr.core.library.metastore._
import org.codefeedr.core.plugin.CollectionPluginFactoryComponent
import org.codefeedr.plugins.{BaseSampleGenerator, GeneratorSourceComponent}
import org.codefeedr.util.EventTime

import scala.reflect.ClassTag
import scala.reflect.runtime.universe

/**
  * Bundle different configuration components
  * These components are not guaranteed to be singleton, due to Flink serializing and deserializing them
  */
trait ConfigurationModule
    extends Serializable
    with FlinkConfigurationProviderComponent
    with KafkaConfigurationComponent
    with ZookeeperConfigurationComponent {
  //The configurationprovider is serializable
  @transient lazy override val zookeeperConfiguration: ZookeeperConfiguration =
    ZookeeperConfiguration(
      connectionString = configurationProvider.get("zookeeper.connectionString"),
      connectionTimeout = configurationProvider.getInt("zookeeper.connectionTimeout", Some(5)),
      sessionTimeout = configurationProvider.getInt("zookeeper.sessionTimeout", Some(30))
    )

  override val configurationProvider: ConfigurationProvider = new ConfigurationProviderImpl()
  @transient lazy override val kafkaConfiguration: KafkaConfiguration =
    new KafkaConfigurationImpl()
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

//All zookeeper nodes
trait ZookeeperComponents
    extends ZkClientComponent
    with ZookeeperConfigurationComponent
    with ConfigurationProviderComponent
    with MetaRootNodeComponent
    with SubjectCollectionNodeComponent
    with SubjectNodeComponent
    with JobNodeCollectionComponent
    with JobNodeComponent
    with EpochCollectionNodeComponent
    with EpochNodeComponent
    with EpochMappingCollectionComponent
    with EpochMappingNodeComponent
    with EpochPartitionCollectionComponent
    with EpochPartitionComponent
    with QuerySinkCollectionComponent
    with QuerySinkNodeComponent
    with ProducerCollectionNodeComponent
    with ProducerNodeComponent
    with QuerySourceCollectionComponent
    with QuerySourceNodeComponent
    with QuerySourceCommandNodeComponent
    with ConsumerCollectionComponent
    with ConsumerNodeComponent
    with SourceSynchronizationStateNodeComponent
    with SourceEpochCollectionComponent
    with SourceEpochNodeComponent {}

trait AbstractCodefeedrComponents
    extends Serializable
    with ConfigurationComponents
    with ZookeeperComponents
    with ZkClientComponent
    with SubjectLibraryComponent
    with KafkaControllerComponent
    with KafkaConsumerFactoryComponent
    with KafkaProducerFactoryComponent
    with SubjectFactoryComponent
    with StreamComposerFactoryComponent
    with EpochStateManagerComponent
    with KafkaTableSinkFactoryComponent
    with GeneratorSourceComponent {}

trait PluginComponents
    extends Serializable
    with AbstractCodefeedrComponents
    with CollectionPluginFactoryComponent {
  override def createCollectionPlugin[
      TData: universe.TypeTag: ClassTag: TypeInformation: EventTime](
      data: Array[TData],
      useTrailedSink: Boolean): CollectionPlugin[TData] =
    new CollectionPlugin[TData](data, useTrailedSink)

}

/**
  * General components performing all the application logic
  */
trait CodefeedrComponents extends AbstractCodefeedrComponents with PluginComponents {
  @transient lazy override implicit val zkClient: ZkClient = new ZkClientImpl()
  @transient lazy override val subjectLibrary: SubjectLibrary = new SubjectLibrary()

  @transient lazy override val kafkaTableSinkFactory = new KafkaTableSinkFactoryImpl()
  @transient lazy override val subjectFactory = new SubjectFactoryController()
  @transient lazy override val streamComposerFactory = new StreamComposerFactory()
  @transient lazy override val epochStateManager = new EpochStateManager()
  @transient lazy override val kafkaController = new KafkaController()

  def createGeneratorSource[TSource](generator: (Long, Long, Long) => BaseSampleGenerator[TSource],
                                     seedBase: Long,
                                     name: String,
                                     eventsPerMillisecond: Option[Long],
                                     enableLogging: Boolean = true): SourceFunction[TSource] =
    new GeneratorSource[TSource](generator, seedBase, name, eventsPerMillisecond, enableLogging)

}
