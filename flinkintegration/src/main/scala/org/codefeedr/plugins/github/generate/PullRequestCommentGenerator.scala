package org.codefeedr.plugins.github.generate

import org.codefeedr.ghtorrent.{Commit, PullRequest, PullRequestComment}
import org.codefeedr.plugins.{BaseEventTimeGenerator, GenerationResponse, WaitForNextCheckpoint}
import org.joda.time.DateTime
import org.codefeedr.plugins.github.generate.EventTimeImpl._

class PullRequestCommentGenerator(seed: Long,
                                  checkpoint: Long,
                                  offset: Long,
                                  pullRequestPerCheckpoint: Int,
                                  val staticEventTime: Option[Long] = None)
    extends BaseEventTimeGenerator[PullRequestComment](seed, checkpoint, offset) {
  private val types = Array("TypeA", "TypeB")
  override val enableEventTime: Boolean = false

  /**
    * Implement to generate a random value
    *
    * @return
    */
  override def generate(): Either[GenerationResponse, PullRequestComment] = {
    Right(
      PullRequestComment(
        pull_request_id = nextCheckpointRelation(pullRequestPerCheckpoint),
        user_id = nextInt(10000),
        comment_id = "", //nextString(10),
        position = nextInt(100),
        body = "", //nextString(50),
        commit_id = nextInt(1000000),
        created_at = nextDateTimeLong(),
        eventTime = None
      ))
  }
}
