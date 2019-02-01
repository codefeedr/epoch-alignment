package org.codefeedr.util

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.concurrent.Future

/**
  * Some utilities making logging of option values easier
  */
object OptionExtensions {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  implicit class DebuggableOption[T](o: Option[T]) {

    private def log(f: String => Unit, hasValue: String, noValue: String): Option[T] =
      o match {
        case Some(_) => f(hasValue); o
        case None => f(noValue); o
      }

    /**
      * Prints a value dependent if the option has a value
      * Returns the option for
      * @param hasValue
      * @param noValue
      * @return
      */
    def info(hasValue: String, noValue: String): Option[T] =
      log(v => {
        if (logger.isInfoEnabled) {
          logger.info(hasValue)
        }
      }, hasValue, noValue)

    /**
      * Debugs a value dependent if the option has a value
      * @param hasValue value to debug if the option has a value
      * @param noValue value to debug if the option has no value
      * @return
      */
    def debug(hasValue: String, noValue: String): Option[T] =
      log(v => {
        if (logger.isDebugEnabled()) {
          logger.debug(hasValue)
        }
      }, hasValue, noValue)
  }
}
