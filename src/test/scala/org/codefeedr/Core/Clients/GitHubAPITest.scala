package org.codefeedr.Core.Clients

import org.codefeedr.Core.Clients.GitHub.GitHubAPI
import org.scalatest.{AsyncFlatSpec, Matchers}

class GitHubAPITest extends AsyncFlatSpec with Matchers {

  "The GitHubAPI" should "connect to GitHub" in {
    val source = new GitHubAPI(1)
    val client = source.client

    client should not be null
    client.getRemainingRequests shouldBe an[Integer]
  }

}
