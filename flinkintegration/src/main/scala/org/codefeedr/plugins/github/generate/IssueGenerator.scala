package org.codefeedr.plugins.github.generate

import org.codefeedr.ghtorrent.Issue
import org.codefeedr.plugins.{BaseEventTimeGenerator, GenerationResponse, WaitForNextCheckpoint}
import org.joda.time.DateTime
import org.codefeedr.plugins.github.generate.EventTimeImpl._

object IssueGenerator {
  val issuesPerCheckpoint: Int = 1000
}

class IssueGenerator(seed: Long,
                     checkpoint: Long,
                     offset: Long,
                     val staticEventTime: Option[DateTime] = None)
    extends BaseEventTimeGenerator[Issue](seed, checkpoint, offset) {
  private val types = Array("TypeA", "TypeB")

  /**
    * Implement to generate a random value
    *
    * @return
    */
  override def generate(): Either[GenerationResponse, Issue] = {
    if (offset < IssueGenerator.issuesPerCheckpoint * (checkpoint + 1)) {
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
      issue_id = nextInt(100),
      pull_request = nextBoolean(),
      pull_request_id = nextCheckpointRelation(PullRequestGenerator.pullRequestPerCheckpoint),
      created_at = nextLong(1000),
      eventTime = getEventTime.getMillis
    )
  }

}
