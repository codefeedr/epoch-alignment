

import org.scalatest._

class GitHubConnectorTest extends AsyncFlatSpec with Matchers {

  "The GitHub connector" should "connect to GitHub" in {
    val gh = new GithubConnector
    val conn = gh.connectToGitHub()

    conn should not be null
    conn.getRemainingRequests shouldBe an[Integer]
  }

  "The GitHub connector" should "connect to the database" in {

    val mongo = new GithubConnector().connectToMongo()
    mongo should not be null
    mongo.count.head.map(r => assert(r >= 0))
  }
}
