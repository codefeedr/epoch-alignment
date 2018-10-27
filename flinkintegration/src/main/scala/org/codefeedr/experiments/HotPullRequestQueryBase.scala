package org.codefeedr.experiments
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{
  EventTimeSessionWindows,
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
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

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

  protected val issueCommentLimiter = Some(1L)
  protected val prCommentlimiter = Some(1L)

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
          enableLogging = false
        ))
      .name("Issue Generator")
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
      .name("Issue Comment Generator")
      .setParallelism(parallelism)

  protected def getPullRequests(parallelism: Int = 1): DataStream[PullRequest] =
    getEnvironment
      .addSource(
        createGeneratorSource(
          (l: Long, c: Long, o: Long) =>
            new PullRequestGenerator(l, c, o, ExperimentConfiguration.prPerCheckpoint),
          HotPullRequestQueryBase.seed3,
          "PullRequestGenerator"))
      .name("PullRequest Generator")
      .setParallelism(parallelism)

  protected def getPullRequestComments(parallelism: Int = 1): DataStream[PullRequestComment] =
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
      .name("PullRequest Comment Generator")
      .setParallelism(parallelism)

  /**
    * Takes a stream of issueComments and composes it into a stream of hotIssues
    *
    * @param input stream of issuecomments
    * @param parallelism parallelism of the job
    * @return
    */
  protected def getDiscussions(
      input: DataStream[IssueComment],
      parallelism: Int = getEnvironment.getParallelism): DataStream[HotIssue] = {
    input
      .map(o => HotIssue(o.issue_id, o.eventTime, 1, None))
      .name("Map to Hot Issue")
      .keyBy(o => o.issueId)
      .window(TumblingEventTimeWindows.of(windowLength))
      .trigger(CountTrigger.of(1))
      .reduce((left, right) => left.merge(right))
      .name("Windowed reduce of Hot Issue")
      .setParallelism(parallelism)
  }

  protected def getHotPullRequests(
      input: DataStream[PullRequestComment],
      parallelism: Int = getEnvironment.getParallelism
  ): DataStream[HotPr] =
    input
      .map(o => HotPr(o.pull_request_id, o.eventTime, 1, 0))
      .name("Map to hot Pullrequest")
      .keyBy(o => o.prId)
      .window(TumblingEventTimeWindows.of(windowLength))
      .trigger(CountTrigger.of(1))
      .reduce((left, right) => left.merge(right))
      .name("Windowed reduce of hot PullRequest")
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
      .name("Merge Hot Issues with PullRequests")
      .setParallelism(parallelism)
  }

  /**
    * Merges the hotIssue and hotPullrequest stream to create hotPullrequests from HotIssues
    *
    * @param hotIssues input stream of hot issues
    * @param parallelism parallelism of created operators
    * @return
    */
  protected def getIssuePullRequests(
      hotIssues: DataStream[IssueCommentPr],
      parallelism: Int = getEnvironment.getParallelism): DataStream[HotPr] = {
    hotIssues
      .map(o => HotPr(o.issueId, o.eventTime, 0, 1))
      .name("Map hot issue to hot PullRequest")
      .keyBy(o => o.prId)
      .window(EventTimeSessionWindows.withGap(windowLength))
      .trigger(CountTrigger.of(1))
      .reduce((left, right) => left.merge(right))
      .name("Reduce hot pullrequests")
      .setParallelism(parallelism)
  }

  /**
    *
    * @param discussions stream of aggregated comment counts
    * @param issues issue input stream
    * @param parallelism parallelism of each created operator
    * @return
    */
  protected def getHotIssues(
      discussions: DataStream[HotIssue],
      issues: DataStream[Issue],
      parallelism: Int = getEnvironment.getParallelism): DataStream[HotIssue] = {
    issues
      .map(o => HotIssue(o.issue_id, o.eventTime, 0, Some(o.pull_request_id)))
      .name("Map Issue to HotIssue")
      .union(discussions)
      .keyBy(o => o.issueId)
      .window(TumblingEventTimeWindows.of(issueJoinWindowLenght))
      .trigger(CountTrigger.of(1))
      .reduce((left, right) => left.merge(right))
      .name("Reduce HotIssues from both sources")
      .setParallelism(parallelism)
      .filter(o => o.prId.nonEmpty)
      .name("Filter late issue comments")
      .setParallelism(parallelism)
  }

  /**
    *   Merges two streams of hotPullrequests in a windowed join
    *
    * @param pullrequests input stream of aggregated pullrequest comment counts
    * @param hotIssues input stream of hot issues
    * @param parallelism parallelism of operators
    * @return
    */
  protected def mergeHotPullRequest(
      pullrequests: DataStream[HotPr],
      hotIssues: DataStream[HotIssue],
      parallelism: Int = getEnvironment.getParallelism): DataStream[HotPr] = {

    val mappedIssues = hotIssues
      .map(o => HotPr(o.prId.get, o.latestEvent, 0, o.comments))
      .name("Map HotIssue to HotPullRequest")
      .setParallelism(parallelism)

    pullrequests
      .union(mappedIssues)
      .keyBy(o => o.prId)
      .window(TumblingEventTimeWindows.of(issueJoinWindowLenght))
      .trigger(CountTrigger.of(1))
      .reduce((l, r) => l.merge(r))
      .name("Reduce hot PullRequests from both sources")
  }

  protected def getHotIssueKafkaSink: SinkFunction[HotIssue] =
    awaitReady(async {
      await(subjectFactory.reCreate[HotIssue]())
      await(subjectFactory.getSink[HotIssue]("HotIssueSink", "HotIssueGenerator"))

    })

  protected def getHotIssueKafkaSource(
      parallelism: Int = getKafkaParallelism): DataStream[HotIssue] = {
    val source = awaitReady(async {
      await(subjectFactory.getSource[HotIssue](s"HotIssueSource", "HotPullrequestQuery"))
    })
    getEnvironment.addSource(source).name(s"HotIssueSource").setParallelism(parallelism)
  }
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
