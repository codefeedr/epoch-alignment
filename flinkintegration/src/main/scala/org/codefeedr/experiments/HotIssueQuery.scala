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

class HotIssueQuery extends ExperimentBase with LazyLogging {

  implicit val HotIssueEventTime: EventTime[HotIssue] =
    new EventTime[HotIssue] {
      override def getEventTime(a: HotIssue): Long = a.latestEvent
    }

  def deploy(args: Array[String]): Unit = {

    logger.info("Initializing arguments")
    initialize(args)
    logger.info("Arguments initialized")
    val env = getEnvironment
    val windowLength = Time.seconds(3)

    val issues = env.addSource(
      createGeneratorSource(
        (l: Long, c: Long, o: Long) =>
          new IssueGenerator(l,
                             c,
                             o,
                             ExperimentConfiguration.issuesPerCheckpoint / getParallelism,
                             ExperimentConfiguration.prPerCheckpoint),
        HotIssueQuery.seed1,
        "IssueGenerator"
      ))

    val issueComments = env
      .addSource(
        createGeneratorSource(
          (l: Long, c: Long, o: Long) =>
            new IssueCommentGenerator(l, c, o, ExperimentConfiguration.issuesPerCheckpoint),
          HotIssueQuery.seed2,
          "IssueCommentGenerator")
      )

    val hotIssues = issueComments
      .map(o => HotIssue(o.issue_id, o.eventTime, 1))
      .keyBy(o => o.issueId)
      .window(TumblingEventTimeWindows.of(windowLength))
      //.window(EventTimeSessionWindows.withGap(idleSessionLength))
      .trigger(CountTrigger.of(1))
      .reduce((left, right) => left.merge(right))

    hotIssues.addSink(new LoggingSinkFunction[HotIssue]("HotIssueSink")).setParallelism(6)
    logger.info("Submitting hot issue query job")
    env.execute("HotIssues")

  }

}
