package org.codefeedr.experiments
import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.core.library.internal.LoggingSinkFunction
import org.codefeedr.experiments.model.{HotIssue, HotPr, IssueCommentPr}

object HotPullrequestStandalone {

  def main(args: Array[String]): Unit = {
    val query = new HotPullrequestStandalone()
    query.deploy(args)
  }

}

class HotPullrequestStandalone extends HotPullRequestQueryBase {

  def deploy(args: Array[String]): Unit = {

    logger.info("Initializing arguments")
    initialize(args)
    logger.info("Arguments initialized")
    val env = getEnvironment

    val issueComments = getIssueComments()
    val issues = getIssues()
    val pullRequestComments = getPullRequestComments()

    //val issuePrs = getIssueCommentPullrequest(issues,issueComments)
    val discussions = getDiscussions(issueComments)
    val hotIssues = getHotIssues(discussions, issues)

    val hotPrs = getHotPullRequests(pullRequestComments)

    val merged = mergeHotPullRequest(hotPrs, hotIssues)

    //val sink = new LoggingSinkFunction[HotPr]("HotPrSink")
    val sink = new LoggingSinkFunction[HotPr]("IssueSink", getRun)
    //issues.addSink(o => Console.println(o))
    //hotIssuePrs.addSink(sink)
    merged.addSink(sink).name("Log hot pullrequests")
    //merged.addSink(o => Console.println(o))
    //logger.info("Submitting hot issue query job")
    execute("HotIssues")
  }

}
