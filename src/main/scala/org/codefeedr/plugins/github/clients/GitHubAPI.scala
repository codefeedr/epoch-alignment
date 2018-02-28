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

import com.typesafe.config.{Config, ConfigFactory}
import org.eclipse.egit.github.core.client.GitHubClient
import org.slf4j.{Logger, LoggerFactory}

/**
  * Wrapper class for setting up GitHubAPI connection.
  */
class GitHubAPI(workerNumber: Integer) {

  //default logger
  private lazy val logger : Logger = LoggerFactory.getLogger(getClass.getName)

  //get the codefeedr configuration files
  private lazy val conf: Config = ConfigFactory.load()

  //initialize githubclient
  @transient
  private lazy val _client: GitHubClient = new GitHubClient

  //some getters
  def client = _client

  /**
    * Set the OAuthToken of the GitHub API using the number of the process.
    * @return the o auth key
    */
  def setOAuthToken(): String = {
    val apiKeys = conf.getStringList("codefeedr.input.github.apikeys")
    val index = workerNumber % apiKeys.size() //get index of api key

    val key = apiKeys.get(index)
    client.setOAuth2Token(key)

    logger.info(s"Worker $workerNumber got assigned with key: $key")

    key
  }

  //set the auth token
  setOAuthToken()
}
