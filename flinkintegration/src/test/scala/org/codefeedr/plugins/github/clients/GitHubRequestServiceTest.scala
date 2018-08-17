package org.codefeedr.plugins.github.clients

import com.typesafe.scalalogging.LazyLogging
import org.eclipse.egit.github.core.client.GitHubClient
import org.scalatest.tagobjects.Slow
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.async.Async.async
import scala.collection.JavaConverters._

class GitHubRequestServiceTest extends AsyncFlatSpec with Matchers with LazyLogging {

  ignore should " should parse and return the correct commit" taggedAs (Slow) in async {
    val client = new GitHubClient()
    val requestService = new GitHubRequestService(client)

    val commit = requestService.getCommit("codefeedr/codefeedr", "2439402a43e11b5efa2a680ac31207f2210b63d5")
    assert(commit.files.size == 5)
  }

  ignore should " should parse and return a list of SimpleCommits" taggedAs (Slow) in async {
    val client = new GitHubClient()
    val requestService = new GitHubRequestService(client)

    val commits = requestService.getAllCommits("codefeedr/codefeedr")
    val latestCommit = commits.iterator().next().asScala.head

    latestCommit should not be null
    assert(commits.next().size() > 0)
  }


  ignore should " should parse and return a list of SimpleCommits starting from sha" taggedAs (Slow) in async {
    val client = new GitHubClient()
    val requestService = new GitHubRequestService(client)

    val commits = requestService.getAllCommits("codefeedr/codefeedr", "2439402a43e11b5efa2a680ac31207f2210b63d5")
    val latestCommit = commits.iterator().next().asScala.head

    latestCommit should not be null
    assert(latestCommit.sha == "2439402a43e11b5efa2a680ac31207f2210b63d5")
  }

  ignore should " should parse and return a list of Events" taggedAs (Slow) in async {
    val client = new GitHubClient()
    val requestService = new GitHubRequestService(client)

    val events = requestService.getEvents()
    val latestEvent = events.head

    latestEvent should not be null
    assert(events.size > 0)
  }


}
