package org.codefeedr.plugins.github.operators

import java.util.Date

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.codefeedr.plugins.github.clients.GitHubProtocol._
import org.mockito.{ArgumentCaptor, Mockito}
import org.mockito.Mockito.verify
import org.scalatest.tagobjects.Slow

import scala.async.Async._

class CheckAndForwardCommitTest extends MongoGitHubSpec {

  val collectionName = "github_commits"

  //02/05/2018 @ 10:00am (UTC)
  val earlierDate = {
    val date = new Date()
    date.setTime(1517824800 * 1000)
    date
  }

  ignore should "be set when the CheckAndForwardCommit is initialized" taggedAs (Slow) in async {
    val operator = new CheckAndForwardCommit()
    await(clearCollection(collectionName))
    await(operator.setIndexes())

    val indexes = await {
      operator.
        mongoDB.
        getCollection(collectionName).
        listIndexes().toFuture()
    }

    //find the correct index
    val findIndex = indexes.flatMap(_.find(x => x._1 == "key" && (x._2.asDocument().containsKey("commit.author.date"))))
    assert(findIndex.size == 1)
  }

  ignore should "be retrieved from the DB" taggedAs(Slow) in async {
    //generate commits
    val commit = fakeCommit()
    val commitEarlier = fakeCommit(earlierDate)

    await(clearCollection(collectionName))
    await(insertDocument(collectionName, commit))
    await(insertDocument(collectionName, commitEarlier))

    val operator = new CheckAndForwardCommit()
    await(operator.setIndexes())

    val latestCommit = await(operator.getLatestCommit("codefeedr/codefeedr"))
    assert(latestCommit.getOrElse("") == commitEarlier.sha)
  }

  ignore should "not be retrieved from the DB if it is not there" taggedAs(Slow) in async {
    //generate commits
    val commit = fakeCommit()
    val commitEarlier = fakeCommit(earlierDate)

    await(clearCollection(collectionName))
    await(insertDocument(collectionName, commit))
    await(insertDocument(collectionName, commitEarlier))

    val operator = new CheckAndForwardCommit()
    await(operator.setIndexes())

    val latestCommit = await(operator.getLatestCommit("codefeedr/bestaatniet")) //wrong repo
    assert(latestCommit.getOrElse("") != commitEarlier.sha)
  }

  ignore should "be returned when retrieving from GitHub API when using no beforeSHA" taggedAs(Slow) in async {
    //this data comes from the codefeedr/codefeedr master branch
    val latestCommit = "5f2bd246c8245d83dfc770c989b8879d47e55b1c" //lets hope the master doesn't get destroyed
    val initCommit = "a9231217e41c854d4d65c824a8ddaec5e6bc8529"
    val sizeTillLatest = 241

    //init operator
    val operator = new CheckAndForwardCommit()
    val runContext = mock[RuntimeContext]

    //set context
    operator.setRuntimeContext(runContext)
    operator.open(new Configuration())

    //set empty auth key
    operator.GitHubAPI.client.setOAuth2Token("")

    val commits = operator.retrieveUntilLatest("codefeedr/codefeedr", "", latestCommit)

    assert(commits.head.sha == latestCommit)
    assert(commits.size == sizeTillLatest)
    assert(commits.last.sha == initCommit)
  }

  ignore should "be returned when retrieving from GitHub API when using beforeSHA" taggedAs(Slow) in async {
    //this data comes from the codefeedr/codefeedr master branch
    val latestCommit = "5f2bd246c8245d83dfc770c989b8879d47e55b1c"
    val aboveBeforeCommit = "d755223ed008bcc2361ba661ce08ac9d93bc30af"
    val beforeCommit = "6edd09e16db14712d1e3a7cbc5ef868ed326f347"
    val sizeTillBefore = 10 //excluding before

    //init operator
    val operator = new CheckAndForwardCommit()
    val runContext = mock[RuntimeContext]

    //set context
    operator.setRuntimeContext(runContext)
    operator.open(new Configuration())

    //set empty auth key
    operator.GitHubAPI.client.setOAuth2Token("")

    val commits = operator.retrieveUntilLatest("codefeedr/codefeedr", beforeCommit, latestCommit)

    assert(commits.head.sha == latestCommit) // latest commit should be the head
    assert(commits.size == sizeTillBefore) //correct size
    assert(commits.last.sha == aboveBeforeCommit) //last commit is the sha above the before sha
    assert(commits.find(x => x.sha == beforeCommit).isEmpty) //before commit isnt there
  }


  /**
  "The asyncInvoke" should "return the correct data in the resultFuture given there is already a before stored." taggedAs(Slow) in async {
    //this data comes from the codefeedr/codefeedr master branch
    val latestCommit = "5f2bd246c8245d83dfc770c989b8879d47e55b1c"
    val beforeCommit = "6edd09e16db14712d1e3a7cbc5ef868ed326f347"
    val sizeTillBefore = 10 //excluding before

    //prepare database on before commit
    val fakeCommitBefore = fakeCommitEarlier.copy(sha = "")
    await(clearCollection(collectionName))
    await(insertDocument(collectionName, fakeCommitBefore))

    //init operator
    val operator = new CheckAndForwardCommit()
    val runContext = mock[RuntimeContext]

    //setup mocking environment
    val mockFuture = mock[ResultFuture[SimpleCommit]]

    //argument captor
    val captor : ArgumentCaptor[java.util.Collection[SimpleCommit]] = ArgumentCaptor.forClass(classOf[java.util.Collection[SimpleCommit]])

    //set context
    operator.setRuntimeContext(runContext)
    operator.open(new Configuration())

    //set empty auth key
    operator.GitHubAPI.client.setOAuth2Token("")

    //async invoke of the event
    operator.asyncInvoke(fakePush, mockFuture)

    //wait and capture
    verify(mockFuture, Mockito.timeout(10000)).complete(captor.capture())

    //assert that correct amount of commits is forwarded
    assert(captor.getValue.size() == sizeTillBefore)
  }

  "The asyncInvoke" should "return the correct data in the resultFuture given there is nothing stored." taggedAs(Slow) in async {
    //this data comes from the codefeedr/codefeedr master branch
    val latestCommit = "5f2bd246c8245d83dfc770c989b8879d47e55b1c"
    val sizeTillBefore = 241

    await(clearCollection(collectionName))

    //init operator
    val operator = new CheckAndForwardCommit()
    val runContext = mock[RuntimeContext]

    //setup mocking environment
    val mockFuture = mock[ResultFuture[SimpleCommit]]

    //argument captor
    val captor : ArgumentCaptor[java.util.Collection[SimpleCommit]] = ArgumentCaptor.forClass(classOf[java.util.Collection[SimpleCommit]])

    //set context
    operator.setRuntimeContext(runContext)
    operator.open(new Configuration())

    //set empty auth key
    operator.GitHubAPI.client.setOAuth2Token("")

    //async invoke of the event
    operator.asyncInvoke(fakePush, mockFuture)

    //wait and capture
    verify(mockFuture, Mockito.timeout(10000)).complete(captor.capture())

    //assert that correct amount of commits is forwarded
    assert(captor.getValue.size() == sizeTillBefore)
  }

  "The asyncInvoke" should "return the correct data in the resultFuture given there is a before stored but more than 20 commits" taggedAs(Slow) in async {
    //this data comes from the codefeedr/codefeedr master branch
    val latestCommit = "5f2bd246c8245d83dfc770c989b8879d47e55b1c"
    val beforeCommit = "6edd09e16db14712d1e3a7cbc5ef868ed326f347"
    val sizeTillBefore = 10 //excluding before

    //prepare database on before commit
    val fakeCommitBefore = fakeCommitEarlier.copy(sha = "6edd09e16db14712d1e3a7cbc5ef868ed326f347")
    await(clearCollection(collectionName))
    await(insertDocument(collectionName, fakeCommitBefore))

    //init operator
    val operator = new CheckAndForwardCommit()
    val runContext = mock[RuntimeContext]

    //setup mocking environment
    val mockFuture = mock[ResultFuture[SimpleCommit]]

    //argument captor
    val captor : ArgumentCaptor[java.util.Collection[SimpleCommit]] = ArgumentCaptor.forClass(classOf[java.util.Collection[SimpleCommit]])

    //set context
    operator.setRuntimeContext(runContext)
    operator.open(new Configuration())

    //set empty auth key
    operator.GitHubAPI.client.setOAuth2Token("")

    //async invoke of the event
    operator.asyncInvoke(fakePush.copy(payload = fakePush.payload.copy(size = 21)), mockFuture)

    //wait and capture
    verify(mockFuture, Mockito.timeout(10000)).complete(captor.capture())

    //assert that correct amount of commits is forwarded
    assert(captor.getValue.size() == sizeTillBefore)
  }

  "The asyncInvoke" should "return the correct data in the resultFuture given the pushevent.before == before stored" taggedAs(Slow) in async {
    //this data comes from the codefeedr/codefeedr master branch
    val latestCommit = "5f2bd246c8245d83dfc770c989b8879d47e55b1c"
    val beforeCommit = "6edd09e16db14712d1e3a7cbc5ef868ed326f347"
    val sizeInPayload = fakePush.payload.size

    //prepare database on before commit
    val fakeCommitBefore = fakeCommitEarlier.copy(sha = "6edd09e16db14712d1e3a7cbc5ef868ed326f347")
    await(clearCollection(collectionName))
    await(insertDocument(collectionName, fakeCommitBefore))

    //init operator
    val operator = new CheckAndForwardCommit()
    val runContext = mock[RuntimeContext]

    //setup mocking environment
    val mockFuture = mock[ResultFuture[SimpleCommit]]

    //argument captor
    val captor : ArgumentCaptor[java.util.Collection[SimpleCommit]] = ArgumentCaptor.forClass(classOf[java.util.Collection[SimpleCommit]])

    //set context
    operator.setRuntimeContext(runContext)
    operator.open(new Configuration())

    //set empty auth key
    operator.GitHubAPI.client.setOAuth2Token("")

    //async invoke of the event
    operator.asyncInvoke(fakePush.copy(payload = fakePush.payload.copy(before = beforeCommit)), mockFuture)

    //wait and capture
    verify(mockFuture, Mockito.timeout(10000)).complete(captor.capture())

    //assert that correct amount of commits is forwarded
    assert(captor.getValue.size() == sizeInPayload)
  }
    **/


}
