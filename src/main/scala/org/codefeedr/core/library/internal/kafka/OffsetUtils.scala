package org.codefeedr.core.library.internal.kafka

object OffsetUtils {
  /**
    * Compares the source with the reference
    * @param source
    * @param reference
    * @return true if the source has equal or higher values for every index
    * @throws NoSuchElementException when source contains an index that is not present in the target
    */
  def HigherOrEqual(source: Map[Int, Long], reference: Map[Int, Long]): Boolean =
    source.forall(o => reference(o._1) <= o._2)

}
