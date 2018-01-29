package org.codefeedr.Core.Input
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichFunction, RichMapFunction}
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import org.apache.flink.util.Collector
import org.codefeedr.Core.Clients.GitHubProtocol._
import org.codefeedr.Core.Clients.{GitHubAPI, MongoDB}
import org.eclipse.egit.github.core.{IRepositoryIdProvider, RepositoryCommit}
import org.eclipse.egit.github.core.service.CommitService
import org.mongodb.scala.{Completed, Observer}
import org.mongodb.scala.model.Filters._

import scala.async.Async.{async, await}
import scala.concurrent.{Await, ExecutionContext, Future}
import collection.JavaConverters._

class GHRetrieveCommitFunction extends AsyncFunction[(String, CommitSimple), Commit] {

  //loads the github api
  lazy val gitHubAPI: GitHubAPI = new GitHubAPI()

  //loads commit service
  lazy val commitService = new CommitService(gitHubAPI.client)

  /** The context used for the future callbacks */
  implicit lazy val executor: ExecutionContext =
    ExecutionContext.fromExecutor(Executors.directExecutor())

  override def asyncInvoke(input: (String, CommitSimple), future: ResultFuture[Commit]): Unit = {
    val repoName = input._1
    val commit = input._2

    val repoCommit = commitService.getCommit(new IRepositoryIdProvider {
      override def generateId(): String = repoName
    }, commit.sha)

    future.complete(Iterable())
  }

  /**
  def parseCommit(commit: RepositoryCommit, repoName: String): Commit = {
    Commit(
      commit.getUrl,
      commit.getSha,
      repoName,
      commit.getCommit.getAuthor.getName,
      commit.getCommit.getAuthor.getEmail,
      commit.getCommit.getCommitter.getName,
      commit.getCommit.getCommitter.getEmail,
      commit.getCommit.getMessage,
      commit.getCommit.getCommentCount,
      Tree(commit.getCommit.getTree.getSha, commit.getCommit.getTree.getSha),
      parseParents(commit.getParents.asScala.toList)
    )
  }

  def parseParents(parents: List[org.eclipse.egit.github.core.Commit]): List[Parent] = {
    parents.map(parent => Parent(parent.getUrl, parent.getSha))
  }

    **/

}
