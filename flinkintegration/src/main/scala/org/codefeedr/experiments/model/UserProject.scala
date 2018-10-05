package org.codefeedr.experiments.model

case class UserProject(userId: Long,
                       projectId: Long,
                       userLogin: String,
                       projectDescription: String)

case class HotIssue(issueId: Int, latestEvent: Long, count: Int) {
  def merge(other: HotIssue) =
    HotIssue(issueId, math.max(latestEvent, other.latestEvent), count + other.count)
}
