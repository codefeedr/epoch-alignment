package org.codefeedr

import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.slf4j.MDC

object LogDemo extends LazyLogging{

  def main(args: Array[String]) = {
    val fmt = ISODateTimeFormat.dateTime
    MDC.put("eventTime", DateTime.now.toString(fmt))
    logger.info("Testing event time")

    MDC.remove("eventTime")
  }
}
