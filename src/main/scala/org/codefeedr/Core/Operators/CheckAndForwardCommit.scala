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

import com.mongodb.client.model.TextSearchOptions
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}
import org.codefeedr.Core.Clients.GitHub.GitHubProtocol.{Commit, PushEvent, SimpleCommit}
import org.codefeedr.Core.Clients.GitHub.{GitHubAPI, GitHubProtocol, GitHubRequestService}
import org.codefeedr.Core.Clients.MongoDB.MongoDB

import scala.async.Async.{async, await}
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala._
import org.mongodb.scala.model.Indexes.{descending, _}
import com.mongodb.client.model.IndexOptions
import org.apache.flink.runtime.concurrent.Executors
import org.mongodb.scala.model.Filters._
import org.bson.conversions.Bson

import scala.concurrent.{ExecutionContext, Future}
import collection.JavaConverters._

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
  var GitHubAPI: GitHubAPI = _

  //loads the github request service
  var gitHubRequestService: GitHubRequestService = _

  override def open(parameters: Configuration): Unit = {
    //numbering starts from 0 so we want to increment
    val taskId = getRuntimeContext.getIndexOfThisSubtask + 1

    //initiate GitHubAPI
    GitHubAPI = new GitHubAPI(taskId)

    //initiate request service
    gitHubRequestService = new GitHubRequestService(GitHubAPI.client)

    //we want an index on the date, to retrieve the latest commit faster
    SetIndexes()
  }

  def SetIndexes(): Unit = {
    collection
      .createIndex(descending("commit.author.date"), new IndexOptions().unique(true))
      .toFuture()
  }

  //TODO refactor this method in multiple readable method
  override def asyncInvoke(input: PushEvent, resultFuture: ResultFuture[SimpleCommit]): Unit =
    async {
      val latestCommit = await(GetLatestCommit(input.repo.name))
      var returned: Boolean = false

      //if <= than 20 commits & before == latestCommit, then forward only PushEvent commits
      if (input.payload.size <= 20 && latestCommit.sha == input.payload.before) {
        resultFuture.complete(input.payload.commits.map(x => SimpleCommit(x.sha)).asJava)
        returned = true //we found what we had to forward
      } else {
        //get all commits starting from the head
        val allCommits = gitHubRequestService.getAllCommits(input.repo.name, input.payload.head)
        var commitsToForward: List[SimpleCommit] = List()

        //keep iterating through the commit pages if we haven't found the before SHA
        while (allCommits.hasNext && !returned) {

          //get full page
          val commitPage = allCommits.next()

          // foreach commit in the page
          commitPage.asScala.foreach { commit =>
            //if we found the commit that is already stored, then forward everything we found
            if (commit.sha == latestCommit.sha) {
              resultFuture.complete(commitsToForward.asJava)
              returned = true //we found what we had to forward
            } else { //if not the commit that is latest in store, then we should forward
              commitsToForward = commitsToForward :+ commit
            }

          }
        }

        //if we have iterated through all commits and not found the before, then we need to retrieve all commits
        if (!returned) {
          resultFuture.complete(commitsToForward.asJava)
        }
      }
    }

  def GetLatestCommit(repoName: String): Future[SimpleCommit] = {
    //TODO can this be more efficient
    val commit =
      collection
        .find(regex("url", repoName.replace("/", "/\\")))
        .sort(descending("commit.author.date"))
        .first()
        .toFuture()

    commit.map(x => SimpleCommit(x.sha))
  }
}
