package org.codefeedr.Core.Plugin

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.codefeedr.Core.{FullIntegrationSpec, KafkaTest}
import org.codefeedr.Model.SubjectType
import org.scalatest.tagobjects.Slow
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.codefeedr.Core.Library.Internal.Kafka.Sink.{KafkaGenericSink, KafkaTableSink}
import org.codefeedr.Core.Library.Internal.Kafka.Source.{KafkaRowSource, KafkaSource, KafkaTableSource}
import org.codefeedr.Core.Library.SubjectFactory
import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase}

import scala.async.Async.{async, await}
import scala.concurrent.{Await, Future}


/**
  * Created by Wouter Zorgdrager.
  * Date: 23-01-18
  * Project: codefeedr
  */

case class PushCounter(id : String, counter : Integer)

class GitHubPluginSpec extends FullIntegrationSpec {

  //get the codefeedr configuration files
  lazy val conf: Config = ConfigFactory.load()

  "A GithubPlugin " should " produce a record for each Github PushEvent " taggedAs (Slow, KafkaTest) in {
    val amountOfRequests = 1

    async {
      val githubType = await(RunGitHubEnvironment(amountOfRequests))

      val githubResult = await(AwaitAllData(githubType))

      //add chain of streams
      val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism)

      //create new stream from result of old stream
      val stream = env.addSource(new KafkaRowSource(githubType)).map(x => PushCounter(x.getField(3).asInstanceOf[String],1))
        //keyBy(0).
        //sum(1).
        //filter(_.counter > 1)

      //create new subjecttype
      val subjectType = await(subjectLibrary.GetOrCreateType[PushCounter])

      //create and add new sink
      val sink = await(SubjectFactory.GetSink[PushCounter])
      stream.addSink(sink)

      //run environment
      this.runEnvironment(env)

      //await all data
      val secondResult = await(AwaitAllData(subjectType))

      //println(githubResult.map(x => (x.field(3), 1)).groupBy(_._1).mapValues(_.size))
      //println(secondResult.map(x => (x.field(1),x.field(0))).groupBy(_._1).mapValues(_.size))
      //println(githubResult.size)

      assert(githubResult.size == secondResult.size)
    }
  }

  "A GitHub plugin " should " store and produce a record for each GitHub PushEvent " taggedAs (Slow, KafkaTest) in {
    val amountOfRequests = 1
    val collectionName = "push_events"

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
        .map(x => PushCounter(x.getField(5).asInstanceOf[String],1)).
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
