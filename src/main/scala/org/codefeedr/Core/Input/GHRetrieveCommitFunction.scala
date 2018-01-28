package org.codefeedr.Core.Input

import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import org.codefeedr.Core.Clients.GitHubProtocol.{Commit, PushEvent}
import org.codefeedr.Core.Clients.{GitHubAPI, MongoDB}
import org.eclipse.egit.github.core.service.CommitService

class GHRetrieveCommitFunction extends AsyncFunction[PushEvent, Commit] {

  //loads the github api
  lazy val gitHubAPI: GitHubAPI = new GitHubAPI()

  //loads the connection with mongodb
  lazy val mongoDB: MongoDB = new MongoDB()

  //loads commit service
  lazy val commitService = new CommitService(gitHubAPI.client)

  override def asyncInvoke(input: PushEvent, resultFuture: ResultFuture[Commit]): Unit = {
    //load commits from up and until the head of the push event

    //1. first retrieve the latest commit of a pushevent

    //2. if no found, get every commit starting from the begin of the commit
    //TODO check if you found up and until head

    //3. if found, get every commit starting from the latest commit
    //TODO check if you found up and until head

    //4. store in MongoDB, if successful then collect because no duplicates :)

    //5. collect
  }

}
