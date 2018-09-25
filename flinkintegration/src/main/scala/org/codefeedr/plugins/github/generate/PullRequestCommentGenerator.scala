package org.codefeedr.plugins.github.generate

import org.codefeedr.ghtorrent.{Commit, PullRequest, PullRequestComment}
import org.codefeedr.plugins.{BaseEventTimeGenerator, GenerationResponse, WaitForNextCheckpoint}
import org.joda.time.DateTime
import org.codefeedr.plugins.github.generate.EventTimeImpl._

class PullRequestCommentGenerator(seed: Long,
                                  checkpoint: Long,
                                  offset: Long,
                                  val staticEventTime: Option[DateTime] = None)
    extends BaseEventTimeGenerator[PullRequestComment](seed, checkpoint, offset) {
  private val types = Array("TypeA", "TypeB")

  /**
    * Implement to generate a random value
    *
    * @return
    */
  override def generate(): Either[GenerationResponse, PullRequestComment] = {
    Right(
      PullRequestComment(
        pull_request_id = nextCheckpointRelation(PullRequestGenerator.pullRequestPerCheckpoint),
        user_id = nextInt(10000),
        comment_id = nextString(10),
        position = nextInt(100),
        body = nextString(50),
        commit_id = nextInt(1000000),
        created_at = nextDateTimeLong(),
        eventTime = getEventTime.getMillis
      ))
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
