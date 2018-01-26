package org.codefeedr.Core.Input

import org.apache.flink.streaming.api.functions.async.RichAsyncFunction
import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector
import org.codefeedr.Core.Clients.{GitHubAPI, MongoDB}
import org.codefeedr.Core.Plugin.PushEvent

case class Commit()

class GHRetrieveCommitFunction extends RichAsyncFunction[PushEvent, Commit] {

  //loads the github api
  lazy val GitHubAPI: GitHubAPI = new GitHubAPI()

  //loads the connection with mongodb
  lazy val MongoDB: MongoDB = new MongoDB()

  override def asyncInvoke(input: PushEvent, collector: AsyncCollector[Commit]): Unit = {
    //TODO retrieve correct commits (maybe also add a MongoConnection here)
  }
}
