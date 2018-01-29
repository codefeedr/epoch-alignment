package org.codefeedr.Core.Clients.GitHub

import com.google.gson.{JsonArray, JsonElement, JsonObject}
import com.google.gson.reflect.TypeToken
import org.eclipse.egit.github.core.IRepositoryIdProvider
import org.eclipse.egit.github.core.client.{GitHubClient, GitHubRequest, PageIterator}
import org.eclipse.egit.github.core.client.PagedRequest.PAGE_FIRST
import org.eclipse.egit.github.core.client.PagedRequest.PAGE_SIZE
import org.eclipse.egit.github.core.service.GitHubService
import java.util.List

import org.codefeedr.Core.Clients.GitHub.GitHubProtocol.{Event, PushEvent}

import collection.JavaConversions._

class GitHubRequestService(client: GitHubClient) extends GitHubService(client) {

  def getCommit(repoName: String, sha: String): JsonElement = {
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
    request.setType(classOf[JsonObject])

    return client.get(request).getBody.asInstanceOf[JsonElement]
  }

  def getEvents(): PageIterator[Event] = {
    val request = createPagedRequest[Event](PAGE_FIRST, PAGE_SIZE)
    request.setUri("/events")
    request.setType(new TypeToken[java.util.List[Event]]() {}.getType)
    return createPageIterator(request)
  }

}
