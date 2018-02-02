package org.codefeedr.Core.Clients.GitHub

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.calcite.shaded.com.google.common.collect.Iterables
import org.codefeedr.Core.Clients.GitHub.GitHubProtocol.SimpleCommit
import org.codefeedr.Core.ZkTest
import org.eclipse.egit.github.core.client.GitHubClient
import org.eclipse.egit.github.core.service.RepositoryService
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

  "getAllEvents " should " should parse and return a list of Events" taggedAs (Slow) in async {
    val client = new GitHubClient()
    val requestService = new GitHubRequestService(client)

    val events = requestService.getEvents()
    val latestEvent = events.head

    latestEvent should not be null
    assert(events.size > 0)
  }


}
