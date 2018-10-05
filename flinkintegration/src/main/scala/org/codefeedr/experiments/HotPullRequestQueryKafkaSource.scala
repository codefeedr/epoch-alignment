package org.codefeedr.experiments

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.codefeedr.core.library.internal.LoggingSinkFunction
import org.codefeedr.plugins.github.generate.{
  IssueCommentGenerator,
  IssueGenerator,
  PullRequestCommentGenerator,
  PullRequestGenerator
}
import org.codefeedr.util.EventTime

case class HotPr(prId: Int, latestEvent: Long, count: Int) {
  def merge(other: HotPr) =
    HotPr(prId, math.max(latestEvent, other.latestEvent), count + other.count)
}

class HotPullRequestQueryKafkaSource extends ExperimentBase with LazyLogging {
  val seed1 = 5255059513954133209L
  val seed2 = 7082249863014760913L

  implicit val HotPrEventTime: EventTime[HotPr] =
    new EventTime[HotPr] {
      override def getEventTime(a: HotPr): Long = a.latestEvent
    }

  def deploy(args: Array[String]): Unit = {
    val env = getEnvironment
    initialize(args, env)
    val idleSessionLength = Time.seconds(2)

    val prs = env.addSource(
      createGeneratorSource(
        (l: Long, c: Long, o: Long) =>
          new PullRequestGenerator(l,
                                   c,
                                   o,
                                   ExperimentConfiguration.prPerCheckpoint / getParallelism),
        seed1,
        "PullRequestGenerator"))

    val prComments = env.addSource(
      createGeneratorSource(
        (l: Long, c: Long, o: Long) =>
          new PullRequestCommentGenerator(l, c, o, ExperimentConfiguration.prPerCheckpoint),
        seed2,
        "PullRequestCommentGenerator")
    )

    val hotPrs = prComments
      .map(o => HotPr(o.pull_request_id, o.eventTime, 1))
      .keyBy(o => o.prId)
      .window(EventTimeSessionWindows.withGap(idleSessionLength))
      .trigger(CountTrigger.of(1))
      .reduce((left, right) => left.merge(right))

    hotPrs.addSink(new LoggingSinkFunction[HotPr]("HotPrSink"))
    logger.info("Submitting hot issue query job")
    env.execute("HotIssues")

  }

}
