package org.codefeedr.plugins.github.generate

import org.codefeedr.ghtorrent._
import org.codefeedr.util.EventTime

object EventTimeImpl {
  implicit val CommitEventTime: EventTime[Commit] = new EventTime[Commit] {
    override def getEventTime(a: Commit): Long = a.eventTime
  }
  implicit val ProjectEventTime: EventTime[Project] = new EventTime[Project] {
    override def getEventTime(a: Project): Long = a.eventTime
  }
  implicit val UserEventTime: EventTime[User] = new EventTime[User] {
    override def getEventTime(a: User): Long = a.eventTime
  }
  implicit val PullRequestEventTime: EventTime[PullRequest] = new EventTime[PullRequest] {
    override def getEventTime(a: PullRequest): Long = a.eventTime
  }
  implicit val PullRequestCommentEventTime: EventTime[PullRequestComment] =
    new EventTime[PullRequestComment] {
      override def getEventTime(a: PullRequestComment): Long = a.eventTime
    }
  implicit val IssueEventTime: EventTime[Issue] =
    new EventTime[Issue] {
      override def getEventTime(a: Issue): Long = a.eventTime
    }
  implicit val IssueCommentEventTime: EventTime[IssueComment] =
    new EventTime[IssueComment] {
      override def getEventTime(a: IssueComment): Long = a.eventTime
    }
}
