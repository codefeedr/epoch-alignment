package org.codefeedr.core

import com.typesafe.config.Config
import org.codefeedr.configuration.{KafkaConfiguration, KafkaConfigurationComponent}
import org.codefeedr.core.engine.query.StreamComposerFactoryComponent
import org.codefeedr.core.library.internal.kafka.KafkaControllerComponent
import org.codefeedr.core.library.{LibraryServices, SubjectFactoryComponent}
import org.codefeedr.core.library.internal.kafka.sink.{EpochStateManager, EpochStateManagerComponent, KafkaProducerFactory, KafkaProducerFactoryComponent}
import org.codefeedr.core.library.internal.kafka.source.{KafkaConsumerFactory, KafkaConsumerFactoryComponent}
import org.codefeedr.core.library.internal.zookeeper.{ZkClient, ZkClientComponent}
import org.codefeedr.core.library.metastore.{SubjectLibrary, SubjectLibraryComponent}

trait IntegrationTestLibraryServices extends ZkClientComponent
  with SubjectLibraryComponent

  with KafkaConsumerFactoryComponent
  with KafkaProducerFactoryComponent
  with KafkaControllerComponent
  with SubjectFactoryComponent
  with StreamComposerFactoryComponent
  with KafkaConfigurationComponent
with EpochStateManagerComponent {
  override val zkClient: ZkClient = LibraryServices.zkClient
  override val subjectLibrary: SubjectLibrary = LibraryServices.subjectLibrary
  override val kafkaConsumerFactory: KafkaConsumerFactory = LibraryServices.kafkaConsumerFactory
  override val kafkaProducerFactory: KafkaProducerFactory = LibraryServices.kafkaProducerFactory
  override val kafkaController: KafkaController = LibraryServices.kafkaController.asInstanceOf[KafkaController]
  val subjectFactory: SubjectFactoryController = LibraryServices.subjectFactory.asInstanceOf[SubjectFactoryController]
  val streamComposerFactory: StreamComposerFactory = LibraryServices.streamComposerFactory.asInstanceOf[StreamComposerFactory]
  override val epochStateManager: EpochStateManager = LibraryServices.epochStateManager
  override val kafkaConfiguration: KafkaConfiguration = LibraryServices.kafkaConfiguration
}
