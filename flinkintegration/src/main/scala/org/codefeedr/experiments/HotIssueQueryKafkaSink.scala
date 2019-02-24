package org.codefeedr.experiments
import org.codefeedr.core.library.internal.LoggingSinkFunction
import org.codefeedr.experiments.model.HotIssue

object HotIssueQueryKafkaSink {

  def main(args: Array[String]): Unit = {
    val query = new HotIssueQueryKafkaSink()
    query.deploy(args)
  }

}

class HotIssueQueryKafkaSink extends HotPullRequestQueryBase {

  def deploy(args: Array[String]): Unit = {
    logger.info("Initializing arguments")
    initialize(args)
    logger.info("Arguments initialized")
    val env = getEnvironment

    val source = getIssueComments()
    val issues = getIssues()

    val discussions = getDiscussions(source)
    val hotIssues = getHotIssues(discussions, issues)

    val discussionSink = new LoggingSinkFunction[HotIssue]("Discussion", getRun)
    discussions.addSink(discussionSink)

    val extraSink = new LoggingSinkFunction[HotIssue]("HotIssue", getRun)
    hotIssues.addSink(extraSink)

    val sink = getHotIssueKafkaSink
    hotIssues.addSink(sink).name("Hot issues to kafka").setParallelism(getKafkaParallelism)
    logger.info("Submitting hot issue query job")

    execute("HotIssues")

  }

}
