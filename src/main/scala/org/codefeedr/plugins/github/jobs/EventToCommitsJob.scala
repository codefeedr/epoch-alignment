package org.codefeedr.plugins.github.jobs

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.codefeedr.core.library.internal.{Job, Plugin}
import org.codefeedr.plugins.github.clients.GitHubProtocol._
import org.codefeedr.plugins.github.input.GitHubSource
import org.codefeedr.plugins.github.operators.{GetOrAddCommit, GetOrAddPushEvent}
import org.apache.flink.streaming.api.datastream.{AsyncDataStream => JavaAsyncDataStream}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.codefeedr.plugins.github.serialization.{
  JsonCommitSerialization,
  JsonPushEventDeserialization
}
import org.json4s.DefaultFormats
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.{
  RichParallelSourceFunction,
  RichSourceFunction
}

import scala.async.Async.async
import scala.concurrent._
import ExecutionContext.Implicits.global

/**
  * This jobs reads push_events from Kafka topic, retrieves commits and forwards it to a new Kafka topic.
  */
class EventToCommitsJob() extends Job[Event, Commit]("events_to_commits_job") {

  //lazily load configuration
  lazy val config = ConfigFactory.load()

  //define kafka/zk related information
  val in_topic = "push_events" //input topic
  val out_topic = "commits" //output topic
  val kafka = config.getString("codefeedr.kafka.server.bootstrap.servers")
  val zKeeper = config.getString("codefeedr.zookeeper.connectionstring")

  @transient //serialization schema for output
  val serSchema = new JsonCommitSerialization()

  @transient //serialization schema for input
  val deSerSchema = new JsonPushEventDeserialization()

  //set parallelism equal to amount of API keys
  override def getParallelism: Int = config.getStringList("codefeedr.input.github.apikeys").size()

  /**
    * Setups a stream for the given environment.
    *
    * @param env the environment to setup the stream on.
    * @return the prepared datastream.
    */
  override def getStream(env: StreamExecutionEnvironment): DataStream[Commit] = {
    val stream =
      env.addSource(setupSource()) //setups the Kafka source

    //map to commits of push event
    val pushStream = stream
      .flatMap(event => event.payload.commits.map(x => (event.repo.name, SimpleCommit(x.sha))))
      .rebalance //rebalances the data over all the workers (round robin)

    //work around for not existing RichAsyncFunction in Scala
    val getCommit = new GetOrAddCommit //get or add commit to mongo
    val finalStream = //10 second timeout with 50 concurrent async requests
      JavaAsyncDataStream.unorderedWait(pushStream.javaStream, getCommit, 10, TimeUnit.SECONDS, 50)

    new DataStream(finalStream)
  }

  /**
    * Composes the execution environment
    * @param env the environment where the source should be composed on.
    * @param queryId the id of this 'query'.
    */
  override def compose(env: StreamExecutionEnvironment, queryId: String): Future[Unit] = async {
    //setup kafka
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", kafka)
    prop.setProperty("group.id", out_topic)
    prop.setProperty("zookeeper.connect", zKeeper)
    prop.setProperty("max.request.size", "10000000") //+- 10 mb

    //setup producer and add sink
    val sink = new FlinkKafkaProducer010[Commit](out_topic, serSchema, prop)
    val stream = getStream(env)
    stream.addSink(sink)
  }

  /**
    * Setups the Kafka source.
    * @return the source linked to the push_event topic.
    */
  def setupSource(): RichParallelSourceFunction[PushEvent] = {
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", kafka)
    prop.setProperty("zookeeper.connect", zKeeper)
    prop.setProperty("group.id", "flink_read_push_events")
    prop.setProperty("fetch.max.message.bytes", "10000000") //+- 10 mb

    new FlinkKafkaConsumer010[PushEvent](in_topic, deSerSchema, prop)
  }
}
