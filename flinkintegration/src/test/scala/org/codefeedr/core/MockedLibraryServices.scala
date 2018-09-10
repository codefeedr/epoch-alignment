package org.codefeedr.core

import com.typesafe.config.Config
import org.codefeedr.configuration._
import org.codefeedr.core.library.internal.kafka.sink.{KafkaProducerFactory, KafkaProducerFactoryComponent}
import org.codefeedr.core.library.internal.kafka.source.{KafkaConsumerFactory, KafkaConsumerFactoryComponent}
import org.codefeedr.core.library.internal.zookeeper.{ZkClient, ZkClientComponent}
import org.codefeedr.core.library.metastore.SubjectLibraryComponent

//Mockito
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.mockito.MockitoSugar

/**
  * Libraryservices with mocks for all components
  */
trait MockedLibraryServices extends ZkClientComponent with MockitoSugar
with SubjectLibraryComponent
with KafkaConsumerFactoryComponent
with KafkaProducerFactoryComponent
with ConfigurationProviderComponent
with KafkaConfigurationComponent
with ZookeeperConfigurationComponent
{
  override val configurationProvider:ConfigurationProvider = mock[ConfigurationProvider]
  override val zkClient: ZkClient = mock[ZkClient]
  override val subjectLibrary: SubjectLibrary = mock[SubjectLibrary]
  override val kafkaConsumerFactory: KafkaConsumerFactory = mock[KafkaConsumerFactory]
  override val kafkaProducerFactory: KafkaProducerFactory = mock[KafkaProducerFactory]
  override val zookeeperConfiguration: ZookeeperConfiguration = mock[ZookeeperConfiguration]
  override val kafkaConfiguration:KafkaConfiguration = mock[KafkaConfiguration]
}

