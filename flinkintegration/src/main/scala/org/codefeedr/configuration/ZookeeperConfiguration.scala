package org.codefeedr.configuration

/**
  *
  * @param connectionString connectionString to connect with zookeeper
  * @param connectionTimeout connectionTimeout to zookeeper, in seconds
  * @param sessionTimeout sessionTimeout to zookeeper, in seconds
  */
case class ZookeeperConfiguration(
                                 connectionString:String = "192.168.99.100:9092",
                                 connectionTimeout:Int =5,
                                 sessionTimeout:Int=30
                                 )


