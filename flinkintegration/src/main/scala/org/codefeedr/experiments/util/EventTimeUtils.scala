package org.codefeedr.experiments.util

object EventTimeUtils {

  /**
    *   Merges two optional eventtimes
    * @param left left eventTime
    * @param right right eventTime
    * @return
    */
  def merge(left: Option[Long], right: Option[Long]): Option[Long] = {
    left match {
      case Some(d) =>
        right match {
          case Some(r) => Some(Math.min(d, r))
          case None => Some(d)
        }
      case None => right
    }
  }
}
