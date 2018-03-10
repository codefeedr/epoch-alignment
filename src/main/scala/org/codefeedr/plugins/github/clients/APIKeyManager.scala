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
package org.codefeedr.plugins.github.clients

import com.typesafe.config._
import org.codefeedr.core.library.LibraryServices
import async.Async._
import scala.concurrent._
import ExecutionContext.Implicits.global

import scala.collection.JavaConverters._

class APIKeyManager {

  private lazy val config: Config = ConfigFactory.load()

  private lazy val zkClient = LibraryServices.zkClient

  lazy val keys: List[APIKey] = loadKeys()

  def loadKeys(): List[APIKey] = {
    val apiList = config
      .getObjectList("codefeedr.input.github.keys")
      .asScala
      .map(x => x.toConfig)
      .map(x => new APIKey(x.getString("key"), x.getInt("limit"), System.currentTimeMillis()))

    apiList.toList
  }

  def saveToZK() = async {}

}
