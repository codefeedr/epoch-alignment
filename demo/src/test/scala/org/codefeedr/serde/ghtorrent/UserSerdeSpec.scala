package org.codefeedr.serde

import java.util.Date

import org.codefeedr.ghtorrent.User
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest._

//Namespaces containing serialization logic
import org.codefeedr.serde.GhTorrent._
import org.codefeedr.demo.ghtorrent.Serde.ops._

class GhtorrentSpec extends FlatSpec {
  val sampleApiString =
    "{ \"_id\" : { \"$oid\" : \"5af8eed26480fd228c5bd9ed\" }, \"login\" : \"suriyat\", \"id\" : 39251372, \"avatar_url\" : \"https://avatars0.githubusercontent.com/u/39251372?v=4\", \"gravatar_id\" : \"\", \"url\" : \"https://api.github.com/users/suriyat\", \"html_url\" : \"https://github.com/suriyat\", \"followers_url\" : \"https://api.github.com/users/suriyat/followers\", \"following_url\" : \"https://api.github.com/users/suriyat/following{/other_user}\", \"gists_url\" : \"https://api.github.com/users/suriyat/gists{/gist_id}\", \"starred_url\" : \"https://api.github.com/users/suriyat/starred{/owner}{/repo}\", \"subscriptions_url\" : \"https://api.github.com/users/suriyat/subscriptions\", \"organizations_url\" : \"https://api.github.com/users/suriyat/orgs\", \"repos_url\" : \"https://api.github.com/users/suriyat/repos\", \"events_url\" : \"https://api.github.com/users/suriyat/events{/privacy}\", \"received_events_url\" : \"https://api.github.com/users/suriyat/received_events\", \"type\" : \"User\", \"site_admin\" : false, \"name\" : null, \"company\" : null, \"blog\" : \"\", \"location\" : null, \"email\" : null, \"hireable\" : null, \"bio\" : null, \"public_repos\" : 1, \"public_gists\" : 0, \"followers\" : 0, \"following\" : 0, \"created_at\" : \"2018-05-14T02:02:05Z\", \"updated_at\": 100,\"eventTime\":1000 }"

  "User" should "be be able to serialize and deserialze" in {
    val user = User(
      id = 10,
      login = "loginname",
      name = "username",
      company = "companyname",
      email = "my@email.com",
      created_at = "2018",
      `type` = "type",
      fake = false,
      deleted = false,
      long = Some(1),
      lat = Some(1),
      country_code = Some("NL"),
      state = None,
      city = Some("city"),
      updated_at = DateTime.now(DateTimeZone.UTC).getMillis,
      eventTime = Some(DateTime.now(DateTimeZone.UTC).getMillis))

    val serialized = user.serialize
    val deserialized = serialized.deserialize[User]

    assert(deserialized == user)
  }

  it should "be able to deserialize the string provided by the api" in {
    val deserialized =
      sampleApiString.deserialize[User]
    assert(deserialized.id == 39251372)
  }
}
