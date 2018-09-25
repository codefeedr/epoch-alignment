package org.codefeedr.plugins.github.generate

import org.codefeedr.ghtorrent._
import org.codefeedr.plugins.{BaseEventTimeGenerator, BaseSampleGenerator}
import org.joda.time.DateTime
import org.codefeedr.plugins.github.generate.EventTimeImpl._

class ProjectGenerator(seed: Long,
                       checkpoint: Long,
                       offset: Long,
                       val staticEventTime: Option[DateTime] = None)
    extends BaseEventTimeGenerator[Project](seed, checkpoint, offset) {
  private val types = Array("TypeA", "TypeB")

  /**
    * Implement to generate a random value
    *
    * @return
    */
  override def generate(): Right[Nothing, Project] =
    Right(
      Project(
        id = nextInt(10000),
        url = nextString(16),
        owner_id = nextInt(1000000),
        description = nextString(200),
        language = nextString(6),
        created_at = nextDateTimeLong(),
        forked_from = nextInt(10000),
        deleted = nextBoolean(),
        updated_at = nextDateTimeLong(),
        eventTime = getEventTime.getMillis
      ))
}
