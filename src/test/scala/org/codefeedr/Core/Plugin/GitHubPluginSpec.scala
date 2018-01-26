package org.codefeedr.Core.Plugin

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.codefeedr.Core.{FullIntegrationSpec, KafkaTest}
import org.codefeedr.Model.SubjectType
import org.scalatest.tagobjects.Slow
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.codefeedr.Core.Library.Internal.Kafka.Sink.{KafkaGenericSink, KafkaTableSink}
import org.codefeedr.Core.Library.Internal.Kafka.Source.{KafkaRowSource, KafkaSource, KafkaTableSource}
import org.codefeedr.Core.Library.SubjectFactory

import scala.async.Async.{async, await}
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
import scala.concurrent._
import scala.concurrent.duration._


/**
  * Created by Wouter Zorgdrager.
  * Date: 23-01-18
  * Project: codefeedr
  */

case class PushCounter(id : String, counter : Integer)

class GitHubPluginSpec extends FullIntegrationSpec {

  "A GithubPlugin " should " produce a record for each Github PushEvent " taggedAs (Slow, KafkaTest) in {
    val amountOfRequests = 1

    async {
      val GithubType = await(RunGitHubEnvironment(amountOfRequests))

      val githubResult = await(AwaitAllData(GithubType))

      /**
      //add chain of streams
      val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism)

      //create new stream from result of old stream
      val stream = env.addSource(new KafkaRowSource(GithubType)).map(x => PushCounter(x.getField(3).asInstanceOf[String],1))
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

        **/

      //println(githubResult.map(x => (x.field(3), 1)).groupBy(_._1).mapValues(_.size))
      //println(secondResult.map(x => (x.field(1),x.field(0))).groupBy(_._1).mapValues(_.size))
      println(githubResult.size)

      assert(githubResult.size > 0)

    }
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
