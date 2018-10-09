package org.codefeedr.experiments

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.codefeedr.core.library.internal.LoggingSinkFunction
import org.codefeedr.experiments.model.HotIssue
import org.codefeedr.plugins.github.generate._
import org.codefeedr.util.EventTime

object HotIssueQuery extends ExperimentBase with LazyLogging {
  val seed1 = 3985731179907005257L
  val seed2 = 5326016289737491967L

  def main(args: Array[String]): Unit = {
    val query = new HotIssueQuery()
    query.deploy(args)
  }

}

class HotIssueQuery extends HotPullRequestQueryBase {

  def deploy(args: Array[String]): Unit = {

    logger.info("Initializing arguments")
    initialize(args)
    logger.info("Arguments initialized")
    val env = getEnvironment

    val source = issueComments()
    val hotIssues = getHotIssues(source)
    val sink = new LoggingSinkFunction[HotIssue]("HotIssueSink")
    hotIssues.addSink(sink)
    logger.info("Submitting hot issue query job")
    env.execute("HotIssues")
  }

}
