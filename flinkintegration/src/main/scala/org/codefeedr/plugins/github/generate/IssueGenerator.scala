package org.codefeedr.plugins.github.generate

import org.codefeedr.ghtorrent.Issue
import org.codefeedr.plugins.{BaseEventTimeGenerator, GenerationResponse, WaitForNextCheckpoint}
import org.joda.time.DateTime
import org.codefeedr.plugins.github.generate.EventTimeImpl._

class IssueGenerator(seed: Long,
                     checkpoint: Long,
                     offset: Long,
                     issuesPerCheckpoint: Int,
                     prPerCheckpoint: Int,
                     val staticEventTime: Option[Long] = None)
    extends BaseEventTimeGenerator[Issue](seed, checkpoint, offset) {
  private val types = Array("TypeA", "TypeB")

  override val enableEventTime: Boolean = true

  /**
    * Implement to generate a random value
    *
    * @return
    */
  override def generate(): Either[GenerationResponse, Issue] = {
    if (offset < issuesPerCheckpoint * (checkpoint + 1)) {
      Right(generateIssue())
    } else {
      Left(WaitForNextCheckpoint(checkpoint + 1))
    }
  }

  private def generateIssue(): Issue = {
    Issue(
      id = nextId(),
      repo_id = nextInt(100),
      reporter_id = nextInt(100),
      assignee_id = nextInt(100),
      issue_id = nextId(),
      pull_request = nextBoolean(),
      pull_request_id = nextCheckpointRelation(prPerCheckpoint),
      created_at = nextLong(1000),
      eventTime = getEventTime
    )
  }

}
