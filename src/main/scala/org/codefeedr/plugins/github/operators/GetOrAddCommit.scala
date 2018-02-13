package org.codefeedr.plugins.github.operators

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.configuration.Configuration
import org.codefeedr.plugins.github.clients.{GitHubAPI, GitHubRequestService}
import org.codefeedr.plugins.github.clients.GitHubProtocol.{Commit, SimpleCommit}

import scala.async.Async._
import scala.concurrent.Future

class GetOrAddCommit extends GetOrAddGeneric[(String, SimpleCommit), Commit] {

  //get the codefeedr configuration files
  private lazy val conf: Config = ConfigFactory.load()

  //collection name
  val collectionName = conf.getString("codefeedr.input.github.commits_collection")

  //loads the github api
  var GitHubAPI: GitHubAPI = _

  //loads the github request service
  var gitHubRequestService: GitHubRequestService = _

  /**
    * Called when runtime context is started.
    * @param parameters of this job.
    */
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    //numbering starts from 0 so we want to increment
    val taskId = getRuntimeContext.getIndexOfThisSubtask + 1

    //initiate GitHubAPI
    GitHubAPI = new GitHubAPI(taskId)
    gitHubRequestService = new GitHubRequestService(GitHubAPI.client)
  }

  /**
    * Get the name of the collection to store in.
    * @return the name of the collection.
    */
  override def getCollectionName: String = collectionName

  /**
    * Get the name of the index.
    * @return the name of the index.
    */
  override def getIndexNames: Seq[String] = Seq("url")

  /**
    * Get the value of the index.
    * @param input to retrieve value from.
    * @return the value of the index.
    */
  override def getIndexValues(input: (String, SimpleCommit)): Seq[String] =
    Seq(s"https://api.github.com/repos/${input._1}/commits/${input._2.sha}")

  /**
    * Factory method to retrieve B using A
    * @param input the input variable A.
    * @return the output variable B.
    */
  override def getFunction(input: (String, SimpleCommit)): Option[Commit] = {
    gitHubRequestService.getCommit(input._1, input._2.sha)
  }
}
