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
package org.codefeedr.plugins.github.operators

import com.mongodb.client.model.TextSearchOptions
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}

import scala.async.Async.{async, await}
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala._
import org.mongodb.scala.model.Indexes.{ascending, _}
import com.mongodb.client.model.IndexOptions
import org.apache.flink.runtime.concurrent.Executors
import org.mongodb.scala.model.Filters.regex
import org.bson.conversions.Bson
import org.codefeedr.plugins.github.clients.GitHubProtocol.{Commit, PushEvent, SimpleCommit}
import org.codefeedr.plugins.github.clients.{GitHubAPI, GitHubRequestService, MongoDB}
import org.mongodb.scala.model.Indexes

import scala.concurrent.{ExecutionContext, Future}
import collection.JavaConverters._

/**
  * Checks the commit data we already have of a repository and forward the missing ones.
  */
//TODO: IMPROVE THIS CLASS
class CheckAndForwardCommit extends RichAsyncFunction[PushEvent, SimpleCommit] {

  //get the codefeedr configuration files
  private lazy val conf: Config = ConfigFactory.load()

  /** The context used for the future callbacks */
  implicit lazy val executor: ExecutionContext =
    ExecutionContext.fromExecutor(Executors.directExecutor())

  //collection name
  val collectionName = conf.getString("codefeedr.input.github.commits_collection")

  //get mongodb
  lazy val mongoDB = new MongoDB()

  //get correct collection
  val collection = mongoDB.getCollection[Commit](collectionName)

  //loads the github api
  @transient
  var GitHubAPI: GitHubAPI = _

  //loads the github request service
  @transient
  var gitHubRequestService: GitHubRequestService = _

  /**
    * Setups the async function.
    * @param parameters of this job.
    */
  override def open(parameters: Configuration): Unit = {
    //numbering starts from 0 so we want to increment
    val taskId = getRuntimeContext.getIndexOfThisSubtask + 1

    //initiate GitHubAPI
    GitHubAPI = new GitHubAPI(taskId)

    //initiate request service
    gitHubRequestService = new GitHubRequestService(GitHubAPI.client)

    //we want an index on the date, to retrieve the latest commit faster
    setIndexes()
  }

  /**
    * Set indexes both on the commit date and the url.
    */
  def setIndexes(): Future[String] = {
    collection
      .createIndex(ascending("commit.author.date", "url"))
      .toFuture()
  }

  //TODO refactor this method in multiple readable method
  override def asyncInvoke(input: PushEvent, resultFuture: ResultFuture[SimpleCommit]): Unit =
    async {

      //for now we only want to forward the 'real-time' commits
      resultFuture.complete(input.payload.commits.map(x => SimpleCommit(x.sha)).asJava)
      /**
      val latestCommit = await(getLatestCommit(input.repo.name))

      //no latest found, then set sha to ""
      val latestSha = latestCommit.getOrElse("")

      //if <= than 20 commits & before == latestCommit, then forward only PushEvent commits
      if (input.payload.size <= 20 && latestSha == input.payload.before) {
        resultFuture.complete(input.payload.commits.map(x => SimpleCommit(x.sha)).asJava)
      } else {
        val commitsToForward =
          retrieveUntilLatest(input.repo.name, latestSha, input.payload.head)

        resultFuture.complete(commitsToForward.asJava)
      }
        **/
    }

  /**
    * Retrieve commits from GitHubAPI from (beforeSHA, headSHA] (excluding beforeSHA)
    * @param repoName full name of the repository.
    * @param beforeSHA the before sha, commits are retrieved until this one is found
    * @param headSHA the head sha, commits are retrieved from this point
    * @return a list of commit sha's
    */
  def retrieveUntilLatest(repoName: String,
                          beforeSHA: String,
                          headSHA: String): List[SimpleCommit] = {
    //get all commits starting from the head
    val allCommits = gitHubRequestService.getAllCommits(repoName, headSHA)
    var commitsToForward: List[SimpleCommit] = List()

    //keep iterating through the commit pages if we haven't found the before SHA
    while (allCommits.hasNext) {

      //get full page
      val commitPage = allCommits.next()

      // foreach commit in the page
      commitPage.asScala.foreach { commit =>
        //if we found the commit that is already stored, then forward everything we found
        if (commit.sha == beforeSHA) {
          return commitsToForward
        } else { //if not the commit that is latest in store, then we should forward
          commitsToForward = commitsToForward :+ commit
        }
      }
    }

    //return all commits, if beforeSHA not found
    commitsToForward
  }

  /**
    * Get the latest commit from a repository.
    * @param repoName the repositoy name.
    * @return the optional commit sha.
    */
  def getLatestCommit(repoName: String): Future[Option[String]] = async {
    //TODO can this be more efficient
    val commit = await {
      collection
        .find(regex("url", "/" + repoName.replace("/", "\\/") + "/"))
        .sort(ascending("commit.author.date"))
        .first()
        .toFuture()
    }

    //if not found
    if (commit == null) {
      None
    } else { //else return sha
      Some(commit.sha)
    }
  }
}
