package org.codefeedr.experiments

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.codefeedr.core.library.internal.LoggingSinkFunction
import org.codefeedr.experiments.model.HotPr
import org.codefeedr.plugins.github.generate.{
  IssueCommentGenerator,
  IssueGenerator,
  PullRequestCommentGenerator,
  PullRequestGenerator
}
import org.codefeedr.util.EventTime

class HotPullRequestQueryKafkaSource extends HotPullRequestQueryBase {

  def deploy(args: Array[String]): Unit = {
    initialize(args)
    val env = getEnvironment

    val prComments = getPullRequestComments()
    val hotPrs = getHotPullRequests(prComments)

    hotPrs.addSink(new LoggingSinkFunction[HotPr]("HotPrSink"))
    logger.info("Submitting hot issue query job")
    env.execute("HotIssues")

  }
}
