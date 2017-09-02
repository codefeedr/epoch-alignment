

package org.codefeedr.Core.Library.Internal.Zookeeper

import java.util.concurrent.TimeUnit

import com.twitter.util.{Duration, JavaTimer}
import com.twitter.zk._
import com.typesafe.config.{Config, ConfigFactory}

object ZookeeperConfig {
  @transient lazy val conf: Config = ConfigFactory.load

  /**
    * Constructs a new zookeeper client using configuration.
    * @return the newly constructed zookeeperclient
    */
  def getClient: ZkClient = {
    implicit val timer: JavaTimer = new JavaTimer(true)
    ZkClient(
      conf.getString("codefeedr.zookeeper.connectionstring"),
      Some(Duration(conf.getLong("codefeedr.zookeeper.connectTimeout"), TimeUnit.SECONDS)),
      Duration(conf.getLong("codefeedr.zookeeper.sessionTimeout"), TimeUnit.SECONDS)
    )
  }
}
