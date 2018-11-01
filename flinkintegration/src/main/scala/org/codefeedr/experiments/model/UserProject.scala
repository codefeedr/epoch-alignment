package org.codefeedr.experiments.model

case class UserProject(userId: Long,
                       projectId: Long,
                       userLogin: String,
                       projectDescription: String)

case class HotIssue(issueId: Int, latestEvent: Long, comments: Int, prId: Option[Int]) {
  def merge(other: HotIssue) =
    HotIssue(issueId,
             other.latestEvent,
             comments + other.comments,
             prId.fold(other.prId)(v => Some(v)))
}

case class HotPr(prId: Int, latestEvent: Long, comments: Int, issueComments: Int) {
  def merge(other: HotPr) =
    HotPr(prId, other.latestEvent, comments + other.comments, issueComments + other.issueComments)
}

case class IssueCommentPr(issueId: Int, prId: Int, eventTime: Long)
