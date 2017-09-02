package org.codefeedr.Core.input

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.eclipse.egit.github.core.client.GitHubClient
import org.eclipse.egit.github.core.event.Event
import org.eclipse.egit.github.core.service.EventService
import org.mongodb.scala.{MongoClient, MongoCollection}

import scala.collection.JavaConverters._

/**
  * A Flink source for GitHub events
  *
  * @author Georgios Gousios <gousiosg@gmail.com>
  */
class GithubConnector extends RichSourceFunction[Event] {

  val conf: Config = ConfigFactory.load

  var client: GitHubClient = _
  var db: MongoCollection[Event] = _

  var isRunning = true

  def connectToGitHub(): GitHubClient = {
    val gh = new GitHubClient
    gh.setOAuth2Token(conf.getString("codefeedr.input.github.apikey"))
    gh
  }

  def connectToMongo(): MongoCollection[Event] = {
    db = MongoClient(conf.getString("codefeedr.mongo.url"))
      .getDatabase(conf.getString("codefeedr.mongo.db"))
      .getCollection(conf.getString("codefeedr.input.github.events_collection"))
    db
  }

  override def open(config: Configuration): Unit = {
    client = connectToGitHub()
    db = connectToMongo()
  }

  override def cancel(): Unit = {
    isRunning = false
  }

  override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
    val es = new EventService(client)
    while (isRunning) {
      println(client.getRemainingRequests)
      val it = es.pagePublicEvents(100)
      while (it.hasNext) {
        it.next.asScala.foreach(e => ctx.collectWithTimestamp(e, e.getCreatedAt.getTime))
      }
    }
  }

}
