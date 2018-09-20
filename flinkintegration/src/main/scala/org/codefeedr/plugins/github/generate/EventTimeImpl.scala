package org.codefeedr.plugins.github.generate

import org.codefeedr.ghtorrent.{Commit, Project, User}
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
}
