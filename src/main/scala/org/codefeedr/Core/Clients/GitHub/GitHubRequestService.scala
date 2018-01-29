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

import com.google.gson.reflect.TypeToken
import org.eclipse.egit.github.core.client.{GitHubClient, GitHubRequest, PageIterator}
import org.eclipse.egit.github.core.client.PagedRequest.PAGE_FIRST
import org.eclipse.egit.github.core.client.PagedRequest.PAGE_SIZE
import org.eclipse.egit.github.core.service.GitHubService
import org.codefeedr.Core.Clients.GitHub.GitHubProtocol.{Commit, Event, PushEvent}

import collection.JavaConversions._

/**
  * Requests commit and events into correct case classes.
  * This service is hooked on the GitHubService (dependency).
  * @param client GitHubClient
  */
class GitHubRequestService(client: GitHubClient) extends GitHubService(client) {

  /**
    * Gets a commit by SHA.
    * @param repoName the name of the repository.
    * @param sha the SHA of the commit.
    * @return the commit case class.
    */
  def getCommit(repoName: String, sha: String): Commit = {
    if (sha == null || repoName == null) {
      throw new IllegalArgumentException("Sha or Reponame cannot be null")
    }

    if (sha.length == 0 || repoName.length == 0) {
      throw new IllegalArgumentException("Sha or Reponame cannot be empty")
    }

    val uri: StringBuilder = new StringBuilder("/repos")
    uri.append("/").append(repoName)
    uri.append("/commits")
    uri.append("/").append(sha)

    val request: GitHubRequest = createRequest()
    request.setUri(uri.toString())
    request.setType(new TypeToken[Commit]() {}.getType)

    return client.get(request).getBody.asInstanceOf[Commit]
  }

  /**
    * Gets all events.
    * @return list of all events.
    */
  def getEvents(): PageIterator[Event] = {
    val request = createPagedRequest[Event](PAGE_FIRST, PAGE_SIZE)
    request.setUri("/events")
    request.setType(new TypeToken[java.util.List[Event]]() {}.getType)
    return createPageIterator(request)
  }

}
