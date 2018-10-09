package org.codefeedr.experiments
import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.codefeedr.experiments.model.HotIssue
import org.codefeedr.ghtorrent.{Issue, IssueComment}
import org.codefeedr.plugins.github.generate.{IssueCommentGenerator, IssueGenerator}
import org.codefeedr.util.EventTime

import scala.async.Async.{async, await}
import scala.concurrent.Await

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global

object HotPullRequestQueryBase {
  val seed1 = 3985731179907005257L
  val seed2 = 5326016289737491967L
}

/**
  * Base class containing all seperate components for the hot pullrequest query experiment
  */
class HotPullRequestQueryBase extends ExperimentBase with LazyLogging {

  @transient lazy protected val windowLength = Time.seconds(3)

  implicit val HotIssueEventTime: EventTime[HotIssue] =
    new EventTime[HotIssue] {
      override def getEventTime(a: HotIssue): Long = a.latestEvent
    }

  protected def issues(parallelism: Int = 1): DataStream[Issue] =
    getEnvironment
      .addSource(
        createGeneratorSource(
          (l: Long, c: Long, o: Long) =>
            new IssueGenerator(l,
                               c,
                               o,
                               ExperimentConfiguration.issuesPerCheckpoint / getParallelism,
                               ExperimentConfiguration.prPerCheckpoint),
          HotPullRequestQueryBase.seed1,
          "IssueGenerator"
        ))
      .setParallelism(parallelism)

  protected def issueComments(parallelism: Int = 1): DataStream[IssueComment] =
    getEnvironment
      .addSource(
        createGeneratorSource(
          (l: Long, c: Long, o: Long) =>
            new IssueCommentGenerator(l, c, o, ExperimentConfiguration.issuesPerCheckpoint),
          HotPullRequestQueryBase.seed2,
          "IssueCommentGenerator",
          Some(100L)
        )
      )
      .setParallelism(parallelism)

  /**
    * Takes a stream of issueComments and composes it into a stream of hotIssues
    * @param input stream of issuecomments
    * @param getParallelism parallelism of the job
    * @return
    */
  protected def getHotIssues(
      input: DataStream[IssueComment],
      parallelism: Int = getEnvironment.getParallelism): DataStream[HotIssue] = {
    input
      .map(o => HotIssue(o.issue_id, o.eventTime, 1))
      .keyBy(o => o.issueId)
      .window(TumblingEventTimeWindows.of(windowLength))
      //.window(EventTimeSessionWindows.withGap(idleSessionLength))
      .trigger(CountTrigger.of(1))
      .reduce((left, right) => left.merge(right))
      .setParallelism(parallelism)
  }

  protected def getHotIssueKafkaSink(): SinkFunction[HotIssue] =
    Await.result(async {
      await(subjectFactory.reCreate[HotIssue]())
      await(subjectFactory.getSink[HotIssue]("HotIssueSink", "HotIssueGenerator"))

    }, 5.seconds)
}
