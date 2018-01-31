package org.codefeedr.Core.Plugin

import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.codefeedr.Core.Clients.GitHub.GitHubProtocol.{Commit, PushEvent}
import org.codefeedr.Core.Clients.GitHub.{GitHubProtocol, GitHubRequestService}
import org.codefeedr.Core.Library.SubjectFactory
import org.codefeedr.Core.{FullIntegrationSpec, KafkaTest}
import org.codefeedr.Model.SubjectType
import org.eclipse.egit.github.core.client.GitHubClient
import org.mongodb.scala.{Document, MongoClient, MongoCollection}
import org.scalatest.tagobjects.Slow

import scala.async.Async.{async, await}
import scala.concurrent.Future
case class PushCounter(id : String, counter : Integer)


class GitHubPluginSpec extends FullIntegrationSpec {

  //get the codefeedr configuration files
  lazy val conf: Config = ConfigFactory.load()

  "A GithubPlugin " should " produce a record for each Github PushEvent " taggedAs (Slow, KafkaTest) in {
    val amountOfRequests = 1

    async {
      val githubType = await(RunGitHubEnvironment(amountOfRequests))

      val githubResult = await(AwaitAllData(githubType))
      /**
      //add chain of streams
      val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism)

      //create new stream from result of old stream
      val stream = env.addSource(new KafkaRowSource(githubType)).map(x => PushCounter(x.getField(5).asInstanceOf[String],1))

      //create new subjecttype
      val subjectType = await(subjectLibrary.GetOrCreateType[PushCounter])

      //create and add new sink
      val sink = await(SubjectFactory.GetSink[PushCounter])
      stream.addSink(sink)

      //run environment
      this.runEnvironment(env)

      //await all data
      val secondResult = await(AwaitAllData(subjectType))

        **/
      //assert both data is the same
      assert(githubResult.size > 1)
    }
  }

  /**
  "A GitHub plugin " should " store and produce a record for each GitHub PushEvent " taggedAs (Slow, KafkaTest) in {
    val amountOfRequests = 1
    val collectionName = "github_events"
    async {
      //prepare mongoclient
      val coll = PrepareMongoEnvironment(conf.getString("codefeedr.mongo.db"), collectionName)

      //wait for the drop
      await(coll.drop().toFuture())

      //run github environment en wait for data
      val githubType = await(RunGitHubEnvironment(amountOfRequests))
      val githubResult = await(AwaitAllData(githubType))

      //add chain of streams
      val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism)

      //create new stream from result of old stream
      //this stream filters out all the unique pushevents
      val stream = env.addSource(new KafkaRowSource(githubType))
        .map(x => PushCounter(x.getField(8).asInstanceOf[String],1)).
        keyBy(0).
        sum(1).
        filter(x => x.counter == 1) //filter out unique ones

      //create new subjecttype
      val subjectType = await(subjectLibrary.GetOrCreateType[PushCounter])

      //create and add new sink
      val sink = await(SubjectFactory.GetSink[PushCounter])
      stream.addSink(sink)

      //run environment
      this.runEnvironment(env)

      //await all data
      val uniqueResult = await(AwaitAllData(subjectType))

      //unique result should be equal to size of mongo collection
      assert(uniqueResult.size == await(coll.count().toFuture()))
    }
  }

    **/

  /**
    * Setups Mongo environment.
    * @param database the database name.
    * @param collectionName the collection name.
    * @return the collection.
    */
  def PrepareMongoEnvironment(database : String, collectionName : String) : MongoCollection[Document] = {
    val coll = MongoClient().
      getDatabase(database).
      getCollection(collectionName)

    coll
  }


  def RunGitHubEnvironment(maxRequests : Integer) : Future[SubjectType] = async {
    val t = await(subjectLibrary.GetOrCreateType[PushEvent]())
    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism)
    logger.debug(s"Composing env for ${t.name}")
    await(new GitHubPlugin(maxRequests).Compose(env))
    logger.debug(s"Starting env for ${t.name}")
    env.execute()
    logger.debug(s"Completed env for ${t.name}")
    t
  }

}
