package org.codefeedr.plugins.github.generate

import org.codefeedr.ghtorrent._
import org.codefeedr.plugins.BaseEventTimeGenerator
import org.joda.time.DateTime
import org.codefeedr.plugins.github.generate.EventTimeImpl._

class CommitGenerator(seed: Long, val staticEventTime: Option[DateTime] = None)
    extends BaseEventTimeGenerator[Commit](seed) {
  private val types = Array("TypeA", "TypeB")

  /**
    * Implement to generate a random value
    *
    * @return
    */
  override def generate(checkpoint:Long): Commit = Commit(
    id = nextInt(1000000),
    sha = nextString(16),
    author_id = nextInt(1000000),
    committer_id = nextInt(1000000),
    project_id = nextInt(10000),
    created_at = nextDateTimeLong(),
    eventTime = getEventTime.getMillis
  )
}
