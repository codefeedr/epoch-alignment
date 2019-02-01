package org.codefeedr.plugins.github.clients

import org.scalatest.{AsyncFlatSpec, Matchers}

class GitHubAPITest extends AsyncFlatSpec with Matchers {

  ignore should "connect to GitHub" in {
    val source = new GitHubAPI(1)
    val client = source.client

    client should not be null
    client.getRemainingRequests shouldBe an[Integer]
  }

  ignore should "retrieve the correct OAuthKey based on its worker id" in {
    val source1 = new GitHubAPI(1)
    val source2 = new GitHubAPI(2)
    val source3 = new GitHubAPI(3)
    val source4 = new GitHubAPI(4)

    val key1 = source1.setOAuthToken()
    val key2 = source2.setOAuthToken()
    val key3 = source3.setOAuthToken()
    val key4 = source4.setOAuthToken()

    //there are 3 keys in the configuration (["0", "1", "2"])
    assert(key1 == "1")
    assert(key2 == "2")
    assert(key3 == "0")
    assert(key4 == "1")
  }

}
