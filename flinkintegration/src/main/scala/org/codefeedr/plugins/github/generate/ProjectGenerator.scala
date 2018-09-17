package org.codefeedr.plugins.github.generate

import org.codefeedr.ghtorrent._
import org.codefeedr.plugins.BaseSampleGenerator
import org.joda.time.DateTime

class ProjectGenerator(seed: Int)(implicit eventTime: DateTime)
    extends BaseSampleGenerator[Project](seed)(eventTime) {
  private val types = Array("TypeA", "TypeB")

  /**
    * Implement to generate a random value
    *
    * @return
    */
  override def generate(): Project = Project(
    id = nextInt(10000),
    url = nextString(16),
    owner_id = nextInt(1000000),
    description = nextString(200),
    language = nextString(6),
    created_at = nextDateTime(),
    forked_from = nextInt(10000),
    deleted = nextBoolean(),
    updated_at = nextDateTime(),
    eventTime = getEventTime
  )
}
