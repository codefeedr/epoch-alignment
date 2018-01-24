package org.codefeedr.Core.Plugin

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.codefeedr.Core.{FullIntegrationSpec, KafkaTest}
import org.codefeedr.Model.SubjectType
import org.scalatest.tagobjects.Slow
import org.apache.flink.api.scala._

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
class GitHubPluginSpec extends FullIntegrationSpec {

  "A GithubPlugin " should " produce a record for each Github PushEvent " taggedAs (Slow, KafkaTest) in {
    val amountOfRequests = 1

    async {
      val GithubType = await(RunGitHubEnvironment(amountOfRequests))
      assert(await(AwaitAllData(GithubType)).size > 0)

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
