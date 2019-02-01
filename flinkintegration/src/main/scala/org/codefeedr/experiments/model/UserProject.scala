package org.codefeedr.experiments.model
import org.codefeedr.experiments.util.EventTimeUtils

case class UserProject(userId: Long,
                       projectId: Long,
                       userLogin: String,
                       projectDescription: String)

case class HotIssue(issueId: Int, latestEvent: Option[Long], comments: Int, prId: Option[Int]) {
  def merge(other: HotIssue) =
    HotIssue(issueId,
             EventTimeUtils.merge(other.latestEvent, latestEvent),
             comments + other.comments,
             prId.fold(other.prId)(v => Some(v)))
}

case class HotPr(prId: Int, latestEvent: Option[Long], comments: Int, issueComments: Int) {
  def merge(other: HotPr) =
    HotPr(prId,
          EventTimeUtils.merge(latestEvent, other.latestEvent),
          comments + other.comments,
          issueComments + other.issueComments)
}

case class IssueCommentPr(issueId: Int, prId: Int, eventTime: Option[Long])
