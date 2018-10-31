package org.codefeedr.configuration

trait ZookeeperConfigurationComponent { this: ConfigurationProviderComponent =>

  val zookeeperConfiguration: ZookeeperConfiguration
}

/**
  *
  * @param connectionString connectionString to connect with zookeeper
  * @param connectionTimeout connectionTimeout to zookeeper, in seconds
  * @param sessionTimeout sessionTimeout to zookeeper, in seconds
  */
case class ZookeeperConfiguration(
    connectionString: String,
    connectionTimeout: Int = 30,
    sessionTimeout: Int = 30
)
