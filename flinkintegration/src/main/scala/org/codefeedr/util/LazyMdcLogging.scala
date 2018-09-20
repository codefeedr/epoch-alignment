package org.codefeedr.util

import com.typesafe.scalalogging.LazyLogging
import org.slf4j.MDC

trait LazyMdcLogging extends LazyLogging {
  def getMdcMap: Map[String, String]

  /***
    * Logs the passed event with the passed MDC map
    * @param message log message to display
    * @param event event to log (used in Kibana)
    */
  protected def logWithMdc(message: String, event: String): Unit = {
    val mdcMap = getMdcMap
    mdcMap.foreach(v => MDC.put(v._1, v._2))
    MDC.put("event", event)
    logger.info(message)
    mdcMap.keys.foreach(MDC.remove)
    MDC.remove("event")
  }
}
