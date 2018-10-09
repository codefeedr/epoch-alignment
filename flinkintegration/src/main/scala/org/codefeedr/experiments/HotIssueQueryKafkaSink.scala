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

    val source = issueComments()
    val hotIssues = getHotIssues(source)
    val sink = getHotIssueKafkaSink()
    hotIssues.addSink(sink)
    logger.info("Submitting hot issue query job")
    env.execute("HotIssues")

  }

}
