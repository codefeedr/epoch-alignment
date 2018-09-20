package org.codefeedr.util

import org.joda.time.{DateTime, DateTimeZone}

/**
  * Event time type class
  * @tparam T type implementing event time
  */
trait EventTime[T] {
  def getEventTime(a:T):Long
  def getEventTimeDT(a:T):DateTime =
    new DateTime(getEventTime(a), DateTimeZone.UTC)
}


object EventTime {
  def apply[A](implicit sh: EventTime[A]): EventTime[A] = sh

  implicit class EventTimeOps[A: EventTime](a: A) {
    def getEventTime:Long = EventTime[A].getEventTime(a)
    def getEventTimeDt:DateTime = EventTime[A].getEventTimeDT(a)
  }
}


object NoEventTime {
  implicit def getNoEventTime[T]:EventTime[T] = new EventTime[T] {
    override def getEventTime(a: T): Long = 0L
  }
}
