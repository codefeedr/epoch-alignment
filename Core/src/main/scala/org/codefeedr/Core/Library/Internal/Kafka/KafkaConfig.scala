/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.codefeedr.Core.Library.Internal.Kafka

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by Niels on 11/07/2017.
  */
object KafkaConfig {
  @transient lazy val conf: Config = ConfigFactory.load

  /**
    * Map configuration to java properties
    * Source: https://stackoverflow.com/questions/34337782/converting-typesafe-config-type-to-java-util-properties
    * @param config the config object to convert to properties
    * @return java properties object
    */
  def propsFromConfig(config: Config): Properties = {
    import scala.collection.JavaConversions._

    val props = new Properties()

    val map: Map[String, Object] = config
      .entrySet()
      .map({ entry =>
        entry.getKey -> entry.getValue.unwrapped()
      })(collection.breakOut)

    props.putAll(map)
    props
  }

  /**
    * Get the kafka configuration
    */
  @transient lazy val properties: Properties = propsFromConfig(
    conf.getConfig("codefeedr.kafka.server"))

  @transient lazy val consumerPropertes: Properties = propsFromConfig(
    conf.getConfig("codefeedr.kafka.consumer"))
}
