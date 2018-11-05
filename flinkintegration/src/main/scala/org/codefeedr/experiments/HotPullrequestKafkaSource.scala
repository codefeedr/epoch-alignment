package org.codefeedr.experiments

import org.codefeedr.core.library.internal.LoggingSinkFunction
import org.codefeedr.experiments.model.HotPr

object HotPullrequestKafkaSource {

  def main(args: Array[String]): Unit = {
    val query = new HotPullrequestKafkaSource()
    query.deploy(args)
  }

}

class HotPullrequestKafkaSource extends HotPullRequestQueryBase {

  def deploy(args: Array[String]): Unit = {

    logger.info("Initializing arguments")
    initialize(args)
    logger.info("Arguments initialized")
    val env = getEnvironment

    val pullRequestComments = getPullRequestComments()

    val hotIssues = getHotIssueKafkaSource()

    val hotPrs = getHotPullRequests(pullRequestComments)

    val merged = mergeHotPullRequest(hotPrs, hotIssues)

    //val sink = new LoggingSinkFunction[HotPr]("HotPrSink")
    val sink = new LoggingSinkFunction[HotPr]("HotPullrequests")
    //issues.addSink(o => Console.println(o))
    //hotIssuePrs.addSink(sink)
    merged.addSink(sink)
    //merged.addSink(o => Console.println(o))
    //logger.info("Submitting hot issue query job")
    execute("hot pullrequest kafka source")
  }

}
