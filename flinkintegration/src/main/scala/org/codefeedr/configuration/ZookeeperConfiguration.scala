package org.codefeedr.configuration


trait ZookeeperConfigurationComponent {
  this: ConfigurationProviderComponent =>

  lazy val zookeeperConfiguration:ZookeeperConfiguration = ZookeeperConfiguration(
    connectionString = configurationProvider.get("zookeeper.connectionString"),
    connectionTimeout = configurationProvider.getInt("zookeeper.connectionTimeout",Some(5)),
    sessionTimeout = configurationProvider.getInt("zookeeper.sessionTimeout",Some(30))
  )
}

/**
  *
  * @param connectionString connectionString to connect with zookeeper
  * @param connectionTimeout connectionTimeout to zookeeper, in seconds
  * @param sessionTimeout sessionTimeout to zookeeper, in seconds
  */
case class ZookeeperConfiguration(
                                 connectionString:String,
                                 connectionTimeout:Int=5,
                                 sessionTimeout:Int=30
                                 )


