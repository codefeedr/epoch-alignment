package org.codefeedr.plugins.github.jobs

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.codefeedr.core.library.internal.{Job, Plugin}
import org.codefeedr.plugins.github.clients.GitHubProtocol._
import org.codefeedr.plugins.github.input.GitHubSource
import org.codefeedr.plugins.github.operators.{GetOrAddCommit, GetOrAddPushEvent}
import org.apache.flink.streaming.api.datastream.{AsyncDataStream => JavaAsyncDataStream}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.codefeedr.plugins.github.serialization.AvroCommitSerializationSchema
import org.json4s.DefaultFormats
import org.apache.flink.api.scala._
import scala.async.Async.async
import scala.concurrent._
import ExecutionContext.Implicits.global

class EventToCommitsJob(maxRequests : Int = -1) extends Job[Event, Commit]("events_to_commits_job") {

  lazy val config = ConfigFactory.load()

  //define kafka/zk related information
  val topicId = "commits"
  val kafka = config.getString("codefeedr.kafka.server.bootstrap.servers")
  val zKeeper = config.getString("codefeedr.zookeeper.connectionstring")

  val serSchema = new AvroCommitSerializationSchema()

  override def getParallelism: Int = 20
  /**
    * Setups a stream for the given environment.
    *
    * @param env the environment to setup the stream on.
    * @return the prepared datastream.
    */
  override def getStream(env: StreamExecutionEnvironment): DataStream[Commit] = {
    val source = new GitHubSource(maxRequests)
    val stream =
      env.addSource(source).filter(_.`type` == "PushEvent").map { x =>
        implicit val formats = DefaultFormats
        PushEvent(x.id, x.repo, x.actor, x.org, x.payload.extract[Payload], x.public, x.created_at)
      }

    //work around for not existing RichAsyncFunction in Scala
    val getPush = new GetOrAddPushEvent //get or add push event to mongo
    val pushJavaStream =
      JavaAsyncDataStream.unorderedWait(stream.javaStream, getPush, 10, TimeUnit.SECONDS, 50)

    //map to commits of push event
    val pushStream = new DataStream(pushJavaStream).
      flatMap(event => event.payload.commits.map(x => (event.repo.name, SimpleCommit(x.sha))))


    //work around for not existing RichAsyncFunction in Scala
    val getCommit = new GetOrAddCommit //get or add commit to mongo
    val finalStream =
      JavaAsyncDataStream.unorderedWait(pushStream.javaStream, getCommit, 10, TimeUnit.SECONDS, 100)

    new DataStream(finalStream)
  }

  override def compose(env: StreamExecutionEnvironment, queryId: String): Future[Unit] = async {
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", kafka)
    prop.setProperty("group.id", topicId)
    prop.setProperty("zookeeper.connect", zKeeper)
    prop.setProperty("max.request.size", "10000000") //+- 10 mb

    val sink = new FlinkKafkaProducer010[Commit](topicId, serSchema, prop)
    val stream = getStream(env)
    stream.addSink(sink)
  }
}
