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
package org.codefeedr.core.clients.github

import com.typesafe.config.{Config, ConfigFactory}
import org.eclipse.egit.github.core.client.GitHubClient

/**
  * Wrapper class for setting up GitHubAPI connection.
  */
class GitHubAPI(workerNumber: Integer) {

  //get the codefeedr configuration files
  private lazy val conf: Config = ConfigFactory.load()

  //Github API rate limit
  private val _rateLimit: Integer = 5000

  //initialize githubclient
  @transient
  private lazy val _client: GitHubClient = new GitHubClient

  //some getters
  def client = _client
  def rateLimit = _rateLimit

  /**
    * Set the OAuthToken of the GitHub API using the number of the process.
    * @return the o auth key
    */
  def setOAuthToken(): String = {
    val apiKeys = conf.getStringList("codefeedr.input.github.apikeys")
    val index = workerNumber % apiKeys.size() //get index of api key

    val key = apiKeys.get(index)
    client.setOAuth2Token(key)

    key
  }

  //set the auth token
  setOAuthToken()
}
