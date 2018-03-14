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

import java.io.IOException

import com.google.gson.reflect.TypeToken
import com.google.gson.{Gson, JsonElement}
import org.codefeedr.plugins.github.clients.GitHubProtocol.{Commit, Event, SimpleCommit}
import org.eclipse.egit.github.core.client.{GitHubClient, GitHubRequest, GitHubResponse, PageIterator}
import org.eclipse.egit.github.core.service.GitHubService
import org.json4s.DefaultFormats
import org.eclipse.egit.github.core.client.PagedRequest.{PAGE_FIRST, PAGE_SIZE}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Requests commit and events into correct case classes.
  * This service is hooked on the GitHubService (dependency).
  *
  * @param client GitHubClient
  */
class GitHubRequestService(client: GitHubClient) extends GitHubService(client) {

  //default logger
  private lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  //should not exceed this margin in terms of requests per hour
  private lazy val githubLimit = 5000

  //key manager
  private lazy val keyManager = new APIKeyManager

  //the current key used
  private var currentKey : Option[APIKey] = None

  // Brings in default date formats etc
  implicit val formats = DefaultFormats

  //use gson to convert back to string
  // TODO: pretty inefficient to first parse and then 'unparse'?
  lazy val gson: Gson = new Gson()

  /**
    * Updates the rate limit by doing a 'free' request to the /rate_limit endpoint.
    * @return (requestsLeft, resetTime
    */
  def updateRateLimit() : (Int, Long) = {
    val url = "/rate_limit" //request to this address doesn't count for the rate limit

    try {
      //by doing this 'free' request, the rate limit is automatically updated by the underlying library (e-git).
      val request: GitHubRequest = createRequest()
      request.setUri(url)
      request.setType(new TypeToken[JsonElement]() {}.getType)
      val response = client.get(request) //do the request

      (response.getHeader("X-RateLimit-Remaining").toInt, response.getHeader("X-RateLimit-Reset").toLong)
    } catch {
      case e: Exception =>  {
        logger.error(s"Error while updating the rate limit: ${e.getMessage}")
        (0, 0)
      }
    }
  }

  /**
    * Gets a commit by SHA.
    * @param repoName the name of the repository.
    * @param sha the SHA of the commit.
    * @return the commit case class.
    */
  @throws(classOf[Exception])
  def getCommit(repoName: String, sha: String): Option[Commit] = {
    if (!retrieveKey()) { //if key can't be acquired, return empty list
      return None
    }

    val uri: StringBuilder = new StringBuilder("/repos")
    uri.append("/").append(repoName)
    uri.append("/commits")
    uri.append("/").append(sha)

    var commit = new JsonElement {}

    var toReturn: Option[Commit] = None

    try {
      //setup request and parse response into a Commit case class
      val request: GitHubRequest = createRequest()
      request.setUri(uri.toString())
      request.setType(new TypeToken[JsonElement]() {}.getType)
      val response: GitHubResponse = client.get(request)

      commit = response.getBody.asInstanceOf[JsonElement]
      toReturn = Some(parse(gson.toJson(commit)).extract[Commit])
    } catch {
      case e: Exception => {
        logger.error(
          s"Error while requesting and parsing a commit: ${e.getMessage} ||| $repoName and $sha")
        toReturn = None
      }
    }

    //release the key
    releaseKey(updateRateLimit())

    //return extracted as Commit
    toReturn
  }

  /**
    * Gets all the commits of a repository.
    * @param repoName the name of the repo.
    * @return a list of all (simple) commit information.
    */
  @throws(classOf[IOException])
  def getAllCommits(repoName: String): PageIterator[SimpleCommit] = {
    if (!retrieveKey()) { //if key can't be acquired, return empty list
      throw new IOException("Can't acquire API key so can't create an iterator for the SimpleCommits.")
    }

    val uri: StringBuilder = new StringBuilder("/repos")
    uri.append("/").append(repoName)
    uri.append("/commits")

    val request = createPagedRequest[SimpleCommit](PAGE_FIRST, PAGE_SIZE)
    request.setUri(uri.toString())
    request.setType(new TypeToken[java.util.List[SimpleCommit]]() {}.getType)

    //return page iterator for all commits
    val iterator = createPageIterator(request)

    //release the key
    releaseKey(updateRateLimit())

    return iterator
  }

  /**
    * Gets all the commits of a repository.
    * @param repoName the name of the repo.
    * @param sha to start from.
    * @return a list of all (simple) commit information.
    */
  @throws(classOf[IOException])
  def getAllCommits(repoName: String, sha: String): PageIterator[SimpleCommit] = {
    if (!retrieveKey()) { //if key can't be acquired, return empty list
      throw new IOException("Can't acquire API key so can't create an iterator for the SimpleCommits.")
    }

    val uri: StringBuilder = new StringBuilder("/repos")
    uri.append("/").append(repoName)
    uri.append("/commits")

    val request = createPagedRequest[SimpleCommit](PAGE_FIRST, PAGE_SIZE)
    request.setUri(uri.toString())
    request.setParams(Map("sha" -> sha).asJava)
    request.setType(new TypeToken[java.util.List[SimpleCommit]]() {}.getType)

    //return page iterator for all commits
    val iterator = createPageIterator(request)

    //release the key
    releaseKey(updateRateLimit())

    return iterator
  }

  /**
    * Gets all events.
    * @return list of all events.
    */
  @throws(classOf[IOException])
  def getEvents(): List[Event] = {
    if (!retrieveKey()) { //if key can't be acquired, return empty list
      return List()
    }

    val request = createPagedRequest[JsonElement](PAGE_FIRST, PAGE_SIZE)
    request.setUri("/events")
    request.setType(new TypeToken[java.util.List[JsonElement]]() {}.getType)

    //get all events
    val events = getAll((createPageIterator(request))).asScala.map(gson.toJson(_))

    //release the key
    releaseKey(updateRateLimit())

    //return extracted into Event class
    return events.map(parse(_).extract[Event]).toList
  }

  /**
    * Retrieves a key with 1000 retries.
    * @return if a key is succesfully retrieved.
    */
  def retrieveKey() : Boolean = {
    var attempts = 1000 //tries a 1000 times

    while (attempts > 0) {
      val key = Await.result(keyManager.acquireKey(), Duration(1, SECONDS)) //get a key

      if (!key.isEmpty) { //if a key is returned update and return true
        currentKey = key
        client.setOAuth2Token(key.get.key)
        return true
      }

      attempts -= 1 //decrement attempts
    }

    logger.error(s"Tried $attempts times to acquire an API key, but it failed. Now skipping this request or throwing exception.")
    currentKey = None //if no key is found reset

    false
  }

  /**
    * Releases a key, so that others processes can use it for their request.
    * @param rateLimit the ratelimit to update.
    */
  def releaseKey(rateLimit : (Int, Long)) = {
    if (currentKey.isEmpty) { //if no key is set..
      logger.warn(s"Can't release a key if no key is acquired.")
    }

    //correct the ratelimit, e.g. we have a max request limit set to 3000, but github allows 5000
    val apiDelta = (githubLimit - currentKey.get.requestLimit)
    val actualRateLimit = rateLimit._1 - apiDelta

    //update key with new requests left
    val keyToUpdate = currentKey.get.copy(requestsLeft = actualRateLimit, resetTime = rateLimit._2)

    //reset current key
    currentKey = None

    //reset key of current client
    client.setOAuth2Token("")

    //release the key afterwards
    Await.result(keyManager.updateAndReleaseKey(keyToUpdate), Duration(1, SECONDS))
  }

}
