package org.codefeedr.plugins.github.generate

import org.codefeedr.ghtorrent.{Commit, PullRequest}
import org.codefeedr.plugins.{BaseEventTimeGenerator, GenerationResponse, WaitForNextCheckpoint}
import org.joda.time.DateTime
import org.codefeedr.plugins.github.generate.EventTimeImpl._

class PullRequestGenerator(seed: Long,
                           checkpoint: Long,
                           offset: Long,
                           pullRequestPerCheckpoint: Int,
                           val staticEventTime: Option[DateTime] = None)
    extends BaseEventTimeGenerator[PullRequest](seed, checkpoint, offset) {
  private val types = Array("TypeA", "TypeB")

  /**
    * Implement to generate a random value
    *
    * @return
    */
  override def generate(): Either[GenerationResponse, PullRequest] = {
    if (offset < pullRequestPerCheckpoint * (checkpoint + 1)) {
      Right(generatePr())
    } else {
      Left(WaitForNextCheckpoint(checkpoint + 1))
    }
  }

  private def generatePr(): PullRequest = {
    PullRequest(
      id = nextId(),
      head_repo_id = nextInt(100),
      base_repo_id = nextInt(100),
      head_commit_id = nextInt(100),
      base_commit_id = nextInt(100),
      pullreq_id = nextId(),
      intra_brach = nextBoolean(),
      eventTime = getEventTime.getMillis
    )
  }

}
