package org.codefeedr.Core.Clients

import org.scalatest.{AsyncFlatSpec, Matchers}

class GitHubAPITest extends AsyncFlatSpec with Matchers {

  "The GitHubAPI" should "connect to GitHub" in {
    val source = new GitHubAPI
    val client = source.client

    client should not be null
    client.getRemainingRequests shouldBe an[Integer]
  }

}
