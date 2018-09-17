package org.codefeedr.plugins

import org.joda.time.DateTime

import scala.util.Random


/**
  * Single use generator for a source element with the passed seed
 *
  * The base class implements a number of utilities useful when creating random elements
  * @param seed
  * @tparam TSource
  */
abstract class BaseSampleGenerator[TSource](val seed:Int) {
  private val random = new Random(seed)

  protected def nextString(length:Int): String = random.alphanumeric.take(length).mkString
  protected def nextInt(maxValue:Int): Int = random.nextInt(maxValue)

  /**
    * Generates a random long value with the passed max value
    * https://stackoverflow.com/questions/2546078/java-random-long-number-in-0-x-n-range
    * @param maxValue maximum generated value
    * @return
    */
  protected def nextLong(maxValue:Long):Long = {
    var bits = 0L
    var result = 0L
    do {
      bits = (random.nextLong << 1) >>> 1
      result = bits % maxValue
    } while ( {
      bits - result + (maxValue - 1) < 0L
    })
    result
  }
  protected def nextBoolean():Boolean = random.nextBoolean()
  protected def nextDateTime():DateTime = new DateTime(random.nextLong())
  protected def nextEmail:String = s"${nextString(6)}@${nextString(6)}.${nextString(3)}"
  protected def randomOf[T](elements: Array[T]):T = elements(random.nextInt(elements.length))



  /**
    * Implement to generate a random value
    * @return
    */
  def generate():TSource

}
