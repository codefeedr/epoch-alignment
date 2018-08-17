package org.codefeedr.core

import com.typesafe.config.Config
import org.codefeedr.configuration.{KafkaConfiguration, KafkaConfigurationComponent}
import org.codefeedr.core.library.internal.kafka.sink.{KafkaProducerFactory, KafkaProducerFactoryComponent}
import org.codefeedr.core.library.internal.kafka.source.{KafkaConsumerFactory, KafkaConsumerFactoryComponent}
import org.codefeedr.core.library.internal.zookeeper.{ZkClient, ZkClientComponent}
import org.codefeedr.core.library.metastore.{SubjectLibrary, SubjectLibraryComponent}

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
with ConfigFactoryComponent
with KafkaConsumerFactoryComponent
with KafkaProducerFactoryComponent
with KafkaConfigurationComponent
{
  override val zkClient: ZkClient = mock[ZkClient]
  override val conf: Config = mock[Config]
  override val subjectLibrary: SubjectLibrary = mock[SubjectLibrary]
  override val kafkaConsumerFactory: KafkaConsumerFactory = mock[KafkaConsumerFactory]
  override val kafkaProducerFactory: KafkaProducerFactory = mock[KafkaProducerFactory]
  override val kafkaConfiguration:KafkaConfiguration = mock[KafkaConfiguration]
}

