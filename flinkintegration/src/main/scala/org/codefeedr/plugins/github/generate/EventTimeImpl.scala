package org.codefeedr.plugins.github.generate

import org.codefeedr.ghtorrent._
import org.codefeedr.util.EventTime

object EventTimeImpl {
  implicit val CommitEventTime: EventTime[Commit] = new EventTime[Commit] {
    override def getEventTime(a: Commit): Option[Long] = a.eventTime
  }
  implicit val ProjectEventTime: EventTime[Project] = new EventTime[Project] {
    override def getEventTime(a: Project): Option[Long] = a.eventTime
  }
  implicit val UserEventTime: EventTime[User] = new EventTime[User] {
    override def getEventTime(a: User): Option[Long] = a.eventTime
  }
  implicit val PullRequestEventTime: EventTime[PullRequest] = new EventTime[PullRequest] {
    override def getEventTime(a: PullRequest): Option[Long] = a.eventTime
  }
  implicit val PullRequestCommentEventTime: EventTime[PullRequestComment] =
    new EventTime[PullRequestComment] {
      override def getEventTime(a: PullRequestComment): Option[Long] = a.eventTime
    }
  implicit val IssueEventTime: EventTime[Issue] =
    new EventTime[Issue] {
      override def getEventTime(a: Issue): Option[Long] = a.eventTime
    }
  implicit val IssueCommentEventTime: EventTime[IssueComment] =
    new EventTime[IssueComment] {
      override def getEventTime(a: IssueComment): Option[Long] = a.eventTime
    }

}
