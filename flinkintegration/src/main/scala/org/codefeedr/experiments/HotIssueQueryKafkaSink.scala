package org.codefeedr.experiments

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

    val sink = getHotIssueKafkaSink
    hotIssues.addSink(sink).name("Hot issues to kafka").setParallelism(getKafkaParallelism)
    logger.info("Submitting hot issue query job")
    execute("HotIssues")

  }

}
