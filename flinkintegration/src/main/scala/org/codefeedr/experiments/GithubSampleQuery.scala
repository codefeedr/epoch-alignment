package org.codefeedr.experiments

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.windowing.assigners.{
  EventTimeSessionWindows,
  TumblingEventTimeWindows,
  TumblingProcessingTimeWindows
}
import org.codefeedr.experiments.model.UserProject
import org.codefeedr.plugins.github.generate._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.codefeedr.ghtorrent.PullRequest

case class CommentCounter(prId: Int, count: Int)
case class PrHotness(prId: Int, eventTime: Long, hotness: Int)

class CommentCounterAggregate extends AggregateFunction[PullRequest, PrHotness, PrHotness] {
  override def createAccumulator(): PrHotness = PrHotness(0, 0, 0)

  override def add(value: PullRequest, accumulator: PrHotness): PrHotness =
    PrHotness(value.id, Math.max(value.eventTime, accumulator.eventTime), accumulator.hotness + 1)

  override def getResult(accumulator: PrHotness): PrHotness = accumulator

  override def merge(a: PrHotness, b: PrHotness): PrHotness =
    PrHotness(a.prId, Math.max(a.eventTime, b.eventTime), a.hotness + b.hotness)
}

class MyProcessWindowFunction
    extends ProcessWindowFunction[CommentCounter, CommentCounter, Int, TimeWindow] {
  override def process(key: Int,
                       context: Context,
                       elements: Iterable[CommentCounter],
                       out: Collector[CommentCounter]): Unit = {
    val hotness = elements.iterator.next()
    out.collect(hotness)
  }
}

class MyTrigger extends Trigger[CommentCounter, TimeWindow] {
  override def onElement(element: CommentCounter,
                         timestamp: Long,
                         window: TimeWindow,
                         ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE

  override def onProcessingTime(time: Long,
                                window: TimeWindow,
                                ctx: Trigger.TriggerContext): TriggerResult =
    TriggerResult.CONTINUE

  override def onEventTime(time: Long,
                           window: TimeWindow,
                           ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

  override def canMerge = true

  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {}
}

/**
  * Sample query using generated github data
  */
object GithubSampleQuery extends ExperimentBase with LazyLogging {
  val seed1 = 1035940093935715931L
  val seed2 = 5548464088400911859L

  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    initialize(args)
    val env = getEnvironment
    // val commits = env.addSource(createGeneratorSource((l:Long) => new CommitGenerator(l),seed1,"CommitGenerator"))
    val pullRequests = env.addSource(
      createGeneratorSource((l: Long, c: Long, o: Long) => new PullRequestGenerator(l, c, o, 1000),
                            seed1,
                            "PullRequestGenerator"))
    val comments =
      env.addSource(
        createGeneratorSource((l: Long, c: Long, o: Long) =>
                                new PullRequestCommentGenerator(l, c, o, 1000),
                              seed2,
                              "PRCommentGenerator"))

    val hotPullRequestIds = comments
      .map(o => CommentCounter(o.pull_request_id, 1))
      .keyBy(o => o.prId)
      .window(EventTimeSessionWindows.withGap(Time.seconds(2)))
      .trigger(new MyTrigger())
      .reduce((p1, p2) => CommentCounter(p1.prId, p1.count + p2.count))
      .filter(o => o.count > 4000)

    /*
    val hotPrs =
      pullRequests
        .join(hotPullRequestIds)
        .where(o => o.id)
        .equalTo(o => o.prId)
        .window(EventTimeSessionWindows.withGap(Time.seconds(2)))
        .apply(
          (pr, comm) =>
            PrHotness(pr.id,pr.eventTime,comm.count)
          )
     */
    hotPullRequestIds.addSink(o => logger.info(o.toString))

    env.execute("userProjectGenerator")
  }

}
