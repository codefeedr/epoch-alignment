package org.codefeedr.plugins.github.generate

import org.codefeedr.ghtorrent._
import org.codefeedr.plugins.BaseSampleGenerator
import org.codefeedr.util.Constants
import org.joda.time.{DateTime, DateTimeZone}

class CommitGenerator(seed: Int)(implicit eventTime: DateTime)
    extends BaseSampleGenerator[Commit](seed)(eventTime) {
  private val types = Array("TypeA", "TypeB")

  /**
    * Implement to generate a random value
    *
    * @return
    */
  override def generate(): Commit = Commit(
    id = nextInt(1000000),
    sha = nextString(16),
    author_id = nextInt(1000000),
    committer_id = nextInt(1000000),
    project_id = nextInt(10000),
    created_at = nextDateTime(),
    eventTime = getEventTime
  )
}
