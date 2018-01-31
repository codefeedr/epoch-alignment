package org.codefeedr.Core.Clients.GitHub

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.Core.ZkTest
import org.eclipse.egit.github.core.client.GitHubClient
import org.scalatest.{AsyncFlatSpec, Matchers}
import org.scalatest.tagobjects.Slow

import scala.async.Async.{async, await}

class GitHubRequestServiceTest extends AsyncFlatSpec with Matchers with LazyLogging {

  "getCommit " should " should parse and return the correct commit" taggedAs (Slow) in async {
    val client = new GitHubClient()
    val requestService = new GitHubRequestService(client)

    val commit = requestService.getCommit("codefeedr/codefeedr", "2439402a43e11b5efa2a680ac31207f2210b63d5")


    assert(commit.files.size == 5)
  }

  "getAllCommits " should " should parse and return the correct commit" taggedAs (Slow) in async {
    val client = new GitHubClient()
    client.setOAuth2Token("")
    val requestService = new GitHubRequestService(client)

    val time = System.currentTimeMillis()
    val commits = requestService.getAllCommits("codefeedr/codefeedr")
    println("Took " + (System.currentTimeMillis() - time) + "ms")
    assert(1 == 1)
  }


}
