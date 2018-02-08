package org.codefeedr.core.clients.GitHub

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.core.clients.GitHub.GitHubProtocol.Commit
import org.eclipse.egit.github.core.client.GitHubClient
import org.scalatest.{AsyncFlatSpec, Matchers}
import org.scalatest.tagobjects.Slow

import scala.async.Async.{async, await}
import collection.JavaConverters._

class GitHubRequestServiceTest extends AsyncFlatSpec with Matchers with LazyLogging {

  "getCommit " should " should parse and return the correct commit" taggedAs (Slow) in async {
    val client = new GitHubClient()
    val requestService = new GitHubRequestService(client)

    val commit = requestService.getCommit("codefeedr/codefeedr", "2439402a43e11b5efa2a680ac31207f2210b63d5")
    assert(commit.files.size == 5)
  }

  "getAllCommits " should " should parse and return a list of SimpleCommits" taggedAs (Slow) in async {
    val client = new GitHubClient()
    val requestService = new GitHubRequestService(client)

    val commits = requestService.getAllCommits("codefeedr/codefeedr")
    val latestCommit = commits.iterator().next().asScala.head

    latestCommit should not be null
    assert(commits.next().size() > 0)
  }


  "getAllCommits with sha " should " should parse and return a list of SimpleCommits starting from sha" taggedAs (Slow) in async {
    val client = new GitHubClient()
    val requestService = new GitHubRequestService(client)

    val commits = requestService.getAllCommits("codefeedr/codefeedr", "2439402a43e11b5efa2a680ac31207f2210b63d5")
    val latestCommit = commits.iterator().next().asScala.head

    latestCommit should not be null
    assert(latestCommit.sha == "2439402a43e11b5efa2a680ac31207f2210b63d5")
  }

  "getAllEvents " should " should parse and return a list of Events" taggedAs (Slow) in async {
    val client = new GitHubClient()
    val requestService = new GitHubRequestService(client)

    val events = requestService.getEvents()
    val latestEvent = events.head

    latestEvent should not be null
    assert(events.size > 0)
  }


}
