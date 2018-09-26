package org.codefeedr.experiments

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.codefeedr.plugins.github.generate._

object HotIssueQuery extends ExperimentBase with LazyLogging {

  val seed1 = 3985731179907005257L
  val seed2 = 5326016289737491967L

  def main(args: Array[String]): Unit = {
    initialize(args)
    val env = getEnvironment

    val idleSessionLength =Time.seconds(2)


    val issues = env.addSource(
      createGeneratorSource((l: Long, c: Long, o: Long) => new IssueGenerator(l, c, o),
                            seed1,
                            "IssueGenerator"))

    val issueComments = env.addSource(
      createGeneratorSource((l: Long, c: Long, o: Long) => new IssueCommentGenerator(l, c, o),
                            seed2,
                            "IssueCommentGenerator")


    )


    val hotIssues = issueComments
      .map(o => (o.issue_id, 1))
      .keyBy(o => o._1)
      .window(EventTimeSessionWindows.withGap(idleSessionLength))
      .trigger(CountTrigger.of(1))
      .reduce((left,right) => (left._1,left._2+right._2))




  }
}
