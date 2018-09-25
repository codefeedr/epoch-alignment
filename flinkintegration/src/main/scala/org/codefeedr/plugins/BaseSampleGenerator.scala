package org.codefeedr.plugins

import org.codefeedr.util.EventTime
import org.codefeedr.util.EventTime._
import org.joda.time.{DateTime, DateTimeZone}

import scala.util.Random

case class GeneratorConfiguration(
    checkpoint: Long,
    offset: Long,
    parallelism: Long,
    parallelIndex: Long
)

trait GenerationResponse {}

/**
  *
  * @param checkpoint the checkpoint to wait for
  */
case class WaitForNextCheckpoint(checkpoint: Long) extends GenerationResponse

/**
  * Single use generator for a source element with the passed seed
  *
  * The base class implements a number of utilities useful when creating random elements
  * @param seed
  * @tparam TSource
  */
abstract class BaseSampleGenerator[TSource](val seed: Long, val checkpoint: Long, val offset: Long) {
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

  protected def nextLongPareto(maxValue: Long, shape: Int = 4): Long =
    (1 to shape).map(_ => nextLong(maxValue)).min

  /**
    * Generates the next id.
    * Currently, offset is used as ID
    * @return
    */
  protected def nextId(): Long = offset

  /**
    * Generates an ID for a relation
    * First picks a random checkpoint, with higher chances to select a recent checkpoint
    * Next selects a random id within the checkpoint, with higher chances to select a lower checkpoint
    * @param checkpointSetSize the number of elements that are generated for each checkpoint
    */
  protected def nextCheckpointRelation(checkpointSetSize: Long): Long = {
    val checkpoint = nextCheckpoint()
    val id = nextLongPareto(checkpointSetSize)
    checkpoint * checkpointSetSize + id
  }

  /**
    * Generates a random past (or current) checkpoint, with a higher chance of selecting more recent checkpoints
    * @return
    */
  protected def nextCheckpoint(): Long = {
    val cp = checkpoint - java.lang.Long.lowestOneBit(random.nextLong())
    if (cp < 0) {
      checkpoint
    }
    cp
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
  def generate(): Either[GenerationResponse, TSource]

  /**
    * Generates a new random value, with event time
    * @return
    */
  def generateWithEventTime(): Either[GenerationResponse, (TSource, Long)] = generate() match {
    case Right(v) => Right(v, getEventTime.getMillis)
    case Left(v) => Left(v)
  }
}

/**
  * Base class for generating elements containing event time
  * Makes sure the event time from the element is used, so there is no difference between the two
  * @param seed
  * @tparam TSource
  */
abstract class BaseEventTimeGenerator[TSource: EventTime](seed: Long,
                                                          checkpoint: Long,
                                                          offset: Long)
    extends BaseSampleGenerator[TSource](seed, checkpoint, offset) {
  override def generateWithEventTime(): Either[GenerationResponse, (TSource, Long)] = {
    generate() match {
      case Right(element) => Right(element, element.getEventTime)
      case Left(v) => Left(v)
    }
  }
}
