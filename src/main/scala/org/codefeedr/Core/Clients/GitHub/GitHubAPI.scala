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
package org.codefeedr.Core.Clients.GitHub

import com.typesafe.config.{Config, ConfigFactory}
import org.eclipse.egit.github.core.client.GitHubClient

/**
  * Wrapper class for setting up GitHubAPI connection.
  */
class GitHubAPI {

  //get the codefeedr configuration files
  private lazy val conf: Config = ConfigFactory.load()

  //Github API rate limit
  private val _rateLimit: Integer = 5000

  //waiting time between request so there are no conflicts with the rate limit
  private val _waitingTime = _rateLimit / 3600

  //initialize githubclient
  @transient
  private lazy val _client: GitHubClient = new GitHubClient

  //some getters
  def client = _client
  def rateLimit = _rateLimit
  def waitingTime = _waitingTime

  /**
    * Set the OAuthToken of the GitHub API.
    */
  def SetOAuthToken() = {
    client.setOAuth2Token(conf.getString("codefeedr.input.github.apikey"))
  }

  //set the auth token
  SetOAuthToken()
}
