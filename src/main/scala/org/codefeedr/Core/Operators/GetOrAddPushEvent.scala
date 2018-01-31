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

package org.codefeedr.Core.Operators

import com.typesafe.config.{Config, ConfigFactory}
import org.codefeedr.Core.Clients.GitHub.GitHubProtocol.PushEvent

class GetOrAddPushEvent extends GetOrAddGeneric[PushEvent, PushEvent]() {

  //get the codefeedr configuration files
  private lazy val conf: Config = ConfigFactory.load()

  //collection name
  val collectionName = conf.getString("codefeedr.input.github.events_collection")

  /**
    * Get the name of the collection.
    * @return the name of the collection.
    */
  override def GetCollectionName: String = {
    collectionName
  }

  /**
    * Get the name of the index.
    * @return the name of the index.
    */
  override def GetIndexName: String = "id"

  /**
    * Get the value of the index.
    * @param input to retrieve value from.
    * @return the value of the index.
    */
  override def GetIndexValue(input: PushEvent): String = input.id

  /**
    * Transforms a PushEvent to the same event.
    * @param input the input variable A.
    * @return the output variable B.
    */
  override def GetFunction(input: PushEvent): PushEvent = input
}
