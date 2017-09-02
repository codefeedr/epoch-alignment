

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
