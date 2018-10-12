package org.codefeedr.experiments
import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{
  EventTimeSessionWindows,
  SlidingEventTimeWindows,
  TumblingEventTimeWindows
}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.util.Collector
import org.codefeedr.experiments.model.{HotIssue, HotPr, IssueCommentPr}
import org.codefeedr.ghtorrent.{Issue, IssueComment, PullRequest, PullRequestComment}
import org.codefeedr.plugins.github.generate.{
  IssueCommentGenerator,
  IssueGenerator,
  PullRequestCommentGenerator,
  PullRequestGenerator
}
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
  val seed3 = 5255059513954133209L
  val seed4 = 7082249863014760913L
}

/**
  * Base class containing all seperate components for the hot pullrequest query experiment
  */
class HotPullRequestQueryBase extends ExperimentBase with LazyLogging {

  @transient lazy protected val windowLength: Time = Time.seconds(3)
  @transient lazy protected val issueJoinWindowLenght: Time = Time.seconds(60)

  protected val issueCommentLimiter = Some(100L)
  protected val prCommentlimiter = Some(100L)

  implicit val HotIssueEventTime: EventTime[HotIssue] =
    new EventTime[HotIssue] {
      override def getEventTime(a: HotIssue): Long = a.latestEvent
    }

  implicit val HotPrEventTime: EventTime[HotPr] =
    new EventTime[HotPr] {
      override def getEventTime(a: HotPr): Long = a.latestEvent
    }

  implicit val IssueCommentPrEventTime: EventTime[IssueCommentPr] =
    new EventTime[IssueCommentPr] {
      override def getEventTime(a: IssueCommentPr): Long = a.eventTime
    }

  protected def getIssues(parallelism: Int = 1): DataStream[Issue] =
    getEnvironment
      .addSource(
        createGeneratorSource(
          (l: Long, c: Long, o: Long) =>
            new IssueGenerator(l,
                               c,
                               o,
                               ExperimentConfiguration.issuesPerCheckpoint,
                               ExperimentConfiguration.prPerCheckpoint),
          HotPullRequestQueryBase.seed1,
          "IssueGenerator",
          None,
          false
        ))
      .setParallelism(parallelism)

  protected def getIssueComments(parallelism: Int = 1): DataStream[IssueComment] =
    getEnvironment
      .addSource(
        createGeneratorSource(
          (l: Long, c: Long, o: Long) =>
            new IssueCommentGenerator(l, c, o, ExperimentConfiguration.issuesPerCheckpoint),
          HotPullRequestQueryBase.seed2,
          "IssueCommentGenerator",
          issueCommentLimiter
        )
      )
      .setParallelism(parallelism)

  protected def getPullRequests(parallelism: Int = 1) =
    getEnvironment
      .addSource(
        createGeneratorSource(
          (l: Long, c: Long, o: Long) =>
            new PullRequestGenerator(l, c, o, ExperimentConfiguration.prPerCheckpoint),
          HotPullRequestQueryBase.seed3,
          "PullRequestGenerator"))
      .setParallelism(parallelism)

  protected def getPullRequestComments(parallelism: Int = 1) =
    getEnvironment
      .addSource(
        createGeneratorSource(
          (l: Long, c: Long, o: Long) =>
            new PullRequestCommentGenerator(l, c, o, ExperimentConfiguration.prPerCheckpoint),
          HotPullRequestQueryBase.seed4,
          "PullRequestCommentGenerator",
          prCommentlimiter
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
      .map(o => HotIssue(o.issue_id, o.eventTime, 1, None))
      .keyBy(o => o.issueId)
      .window(TumblingEventTimeWindows.of(windowLength))
      .trigger(CountTrigger.of(1))
      .reduce((left, right) => left.merge(right))
      .setParallelism(parallelism)
  }

  protected def getHotPullRequests(
      input: DataStream[PullRequestComment],
      parallelism: Int = getEnvironment.getParallelism
  ): DataStream[HotPr] =
    input
      .map(o => HotPr(o.pull_request_id, o.eventTime, 1, 0))
      .keyBy(o => o.prId)
      .window(TumblingEventTimeWindows.of(windowLength))
      .trigger(CountTrigger.of(1))
      .reduce((left, right) => left.merge(right))
      .setParallelism(parallelism)

  protected def getIssueCommentPullrequest(
      issues: DataStream[Issue],
      comments: DataStream[IssueComment],
      parallelism: Int = getEnvironment.getParallelism): DataStream[IssueCommentPr] = {
    issues
      .join(comments)
      .where(o => o.id)
      .equalTo(o => o.issue_id)
      .window(TumblingEventTimeWindows.of(issueJoinWindowLenght))
      .trigger(CountTrigger.of(1))
      .apply((i, c) => IssueCommentPr(i.id, i.pull_request_id, Math.max(i.eventTime, c.eventTime)))
      .setParallelism(parallelism)
  }

  /**
    * Merges the hotIssue and hotPullrequest stream to create hotPullrequests from HotIssues
    * @param hotIssues
    * @param issues
    * @param parallelism
    * @return
    */
  protected def getIssuePullRequests(
      hotIssues: DataStream[IssueCommentPr],
      parallelism: Int = getEnvironment.getParallelism): DataStream[HotPr] = {
    hotIssues
      .map(o => HotPr(o.issueId, o.eventTime, 0, 1))
      .keyBy(o => o.prId)
      .window(EventTimeSessionWindows.withGap(windowLength))
      .trigger(CountTrigger.of(1))
      .reduce((left, right) => left.merge(right))
      .setParallelism(parallelism)
  }

  /**
    *
    * @param hotIssues hot issues input stream
    * @param issues issue input stream
    * @param parallelism parallelism of each created operator
    * @return
    */
  protected def getHotIssuePullRequests(hotIssues: DataStream[HotIssue],
                                        issues: DataStream[Issue],
                                        parallelism: Int = getEnvironment.getParallelism) = {
    issues
      .map(o => HotIssue(o.issue_id, o.eventTime, 0, Some(o.pull_request_id)))
      .union(hotIssues)
      .keyBy(o => o.issueId)
      .window(TumblingEventTimeWindows.of(issueJoinWindowLenght))
      .trigger(CountTrigger.of(1))
      .reduce((left, right) => left.merge(right))
      .setParallelism(parallelism)
      .filter(o => o.prId.nonEmpty)
      .setParallelism(parallelism)
      .map(o => HotPr(o.prId.get, o.latestEvent, 0, o.comments))
      .setParallelism(parallelism)
  }

  /**
    *   Merges two streams of hotPullrequests in a windowed join
    * @param left
    * @param right
    * @param parallelism
    * @return
    */
  protected def mergeHotPullRequest(
      left: DataStream[HotPr],
      right: DataStream[HotPr],
      parallelism: Int = getEnvironment.getParallelism): DataStream[HotPr] = {
    left
      .union(right)
      .keyBy(o => o.prId)
      .window(TumblingEventTimeWindows.of(issueJoinWindowLenght))
      .trigger(CountTrigger.of(1))
      .reduce((l, r) => l.merge(r))
  }

  protected def getHotIssueKafkaSink(): SinkFunction[HotIssue] =
    Await.result(async {
      await(subjectFactory.reCreate[HotIssue]())
      await(subjectFactory.getSink[HotIssue]("HotIssueSink", "HotIssueGenerator"))

    }, 5.seconds)
}

class IssueHotIssueFlatMap extends CoFlatMapFunction[Issue, HotIssue, HotPr] {
  var issue: Option[Issue] = None
  var lastHotIssue: Option[HotIssue] = None

  override def flatMap1(value: Issue, out: Collector[HotPr]): Unit = {
    if (issue.nonEmpty) {
      throw new IllegalStateException("Got the same issue twice")
    }
    issue = Some(value)
    lastHotIssue match {
      case Some(hi) => out.collect(HotPr(value.pull_request_id, value.eventTime, 0, hi.comments))
      case None =>
    }
  }
  override def flatMap2(value: HotIssue, out: Collector[HotPr]): Unit = {
    lastHotIssue = Some(value)
    issue match {
      case Some(i) => out.collect(HotPr(i.pull_request_id, i.eventTime, 0, value.comments))
      case None =>
    }
  }
}
