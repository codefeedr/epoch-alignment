package org.codefeedr.util

import org.joda.time.{DateTime, DateTimeZone}

/**
  * Event time type class
  * @tparam T type implementing event time
  */
trait EventTime[T] extends Serializable {
  def getEventTime(a: T): Option[Long]
  def getEventTimeDT(a: T): Option[DateTime] =
    getEventTime(a).map(o => new DateTime(o, DateTimeZone.UTC))
}

object EventTime {
  def apply[A](implicit sh: EventTime[A]): EventTime[A] = sh

  implicit class EventTimeOps[A: EventTime](a: A) {
    def getEventTime: Option[Long] = EventTime[A].getEventTime(a)
    def getEventTimeDt: Option[DateTime] = EventTime[A].getEventTimeDT(a)
  }
}

object NoEventTime {
  implicit def getNoEventTime[T]: EventTime[T] = new EventTime[T] {
    override def getEventTime(a: T): Option[Long] = None
  }
}
