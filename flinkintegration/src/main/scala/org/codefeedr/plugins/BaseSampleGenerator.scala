package org.codefeedr.plugins

import org.codefeedr.util.EventTime._
import org.joda.time.{DateTime, DateTimeZone}

import scala.util.Random

/**
  * Single use generator for a source element with the passed seed
  *
  * The base class implements a number of utilities useful when creating random elements
  * @param seed
  * @tparam TSource
  */
abstract class BaseSampleGenerator[TSource](val seed: Long) {
  private val random = new Random(seed)
  val staticEventTime: Option[DateTime]

  protected def getEventTime: DateTime = staticEventTime.getOrElse(DateTime.now(DateTimeZone.UTC))
  protected def nextString(length: Int): String = random.alphanumeric.take(length).mkString
  protected def nextInt(maxValue: Int): Int = random.nextInt(maxValue)

  /**
    * Generates a random long value with the passed max value
    * https://stackoverflow.com/questions/2546078/java-random-long-number-in-0-x-n-range
    * @param maxValue maximum generated value
    * @return
    */
  protected def nextLong(maxValue: Long): Long = {
    var bits = 0L
    var result = 0L
    do {
      bits = (random.nextLong << 1) >>> 1
      result = bits % maxValue
    } while ({
      bits - result + (maxValue - 1) < 0L
    })
    result
  }

  protected def nextBoolean(): Boolean = random.nextBoolean()
  protected def nextDateTimeLong(): Long = random.nextLong()
  protected def nextDateTime(): DateTime = new DateTime(nextDateTimeLong())
  protected def nextEmail: String = s"${nextString(6)}@${nextString(6)}.${nextString(3)}"
  protected def randomOf[T](elements: Array[T]): T = elements(random.nextInt(elements.length))

  /**
    * Implement to generate a random value
    * @return
    */
  def generate(): TSource

  /**
    * Generates a new random value, with event time
    * @return
    */
  def generateWithEventTime(): (TSource, Long) = (generate(), getEventTime.getMillis)

}

/**
  * Base class for generating elements containing event time
  * Makes sure the event time from the element is used, so there is no difference between the two
  * @param seed
  * @tparam TSource
  */
abstract class BaseEventTimeGenerator[TSource](seed: Long)
    extends BaseSampleGenerator[TSource](seed) {
  override def generateWithEventTime(): (TSource, Long) = {
    val element = generate()
    (element, element.getEventTime)
  }
}
