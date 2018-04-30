package org.codefeedr.core.library.internal.kafka

import com.typesafe.scalalogging.LazyLogging

object OffsetUtils extends LazyLogging {

  /**
    * Compares the source with the reference
    * @param source
    * @param reference
    * @return true if the source has equal or higher values for every index
    * @throws NoSuchElementException when source contains an index that is not present in the target
    */
  def HigherOrEqual(source: Map[Int, Long], reference: Map[Int, Long]): Boolean = {

    if (source.isEmpty) {
      logger.warn(s"HigherOrEqual for offset comparison called with empty source. Returning true")
      true
    } else {
      source.forall(o => reference(o._1) <= o._2)
    }
  }

}
