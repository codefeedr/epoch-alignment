package org.codefeedr.core.operators

import java.util.Date

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.codefeedr.core.clients.github.GitHubProtocol._
import org.scalatest.tagobjects.Slow
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.any
import org.mockito.{ArgumentCaptor, Mockito}
import org.mongodb.scala.model.Filters
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import scala.async.Async._
import collection.JavaConverters._
import scala.async.Async
import scala.concurrent.Await
import scala.concurrent.duration._

class GetOrAddCommitTest extends MongoDBSpec with Eventually {

  val collectionName = "github_commits"

  val repo = "codefeedr/codefeedr"
  val simpleCommit = SimpleCommit("2439402a43e11b5efa2a680ac31207f2210b63d5")
  val fakeCommit = Commit("2439402a43e11b5efa2a680ac31207f2210b63d5",
    "https://api.github.com/repos/codefeedr/codefeedr/commits/2439402a43e11b5efa2a680ac31207f2210b63d5",
    CommitData(
      CommitUser("wouter", "test", new Date()),
      CommitUser("wouter", "test", new Date()),
      "test",
      Tree("test"),
      1,
      Verification(false, "", None, None)),
    User(1, "wouter", "test", "test", false),
    User(1, "wouter", "test", "test", false),
    Nil,
    Stats(2, 1, 1),
    Nil)

  "The correct indexes" should "be set when the GetOrAddCommit is initialized" taggedAs(Slow) in async {
    val operator = new GetOrAddCommit()
    await(clearCollection(collectionName))
    await(operator.setIndexes(operator.getIndexNames))

    val indexes = await {
      operator.
        mongoDB.
        getCollection(operator.getCollectionName).
        listIndexes().toFuture()
    }

    //find the correct index
    val findIndex = indexes.flatMap(_.find(x => x._1 == "key" && x._2.asDocument().containsKey("url")))
    assert(findIndex.size == 1)
  }

  "The correct API key" should "be set when GetOrAddCommit is initialized" taggedAs(Slow) in async {
    val operator = new GetOrAddCommit()
    val runtimeContext = mock[RuntimeContext]

    //subtask 3
    when(runtimeContext.getIndexOfThisSubtask).thenReturn(2)

    //set runtime context
    operator.setRuntimeContext(runtimeContext)

    //open operator
    operator.open(new Configuration())

    //assert the 3th token is picked (3 modulo 3 == 0)
    assert(operator.GitHubAPI.setOAuthToken() == "0")
  }

  "The correct URL" should "be returned when inputting a commit and reponame" in {
    val operator = new GetOrAddCommit()
    val input = (repo, simpleCommit)

    //call method
    val output = operator.getIndexValues(input)

    //assert it is correct
    assert(output.head == "https://api.github.com/repos/codefeedr/codefeedr/commits/2439402a43e11b5efa2a680ac31207f2210b63d5")
  }

  "A SimpleCommit" should "be retrieved and forwarded if already in DB" in async {
    val operator = new GetOrAddCommit()
    val runtimeContext = mock[RuntimeContext]

    //subtask 3
    when(runtimeContext.getIndexOfThisSubtask).thenReturn(2)

    //set runtime context
    operator.setRuntimeContext(runtimeContext)

    //await the clearing of the collection
    await(clearCollection(collectionName))

    //open operator
    operator.open(new Configuration())

    //insertion
    await(insertDocument(collectionName, fakeCommit))

    //setup mocking environment
    val mockFuture = mock[ResultFuture[Commit]]

    //call invoke
    operator.asyncInvoke((repo, simpleCommit), mockFuture)

    eventually {
      //verify the future has been used
      verify(mockFuture, times(1)).complete(List(fakeCommit).asJavaCollection)

      succeed
    }
  }

  "A SimpleCommit" should "be requested and forwarded if not in DB" in async {
    val operator = spy(new GetOrAddCommit())
    val runtimeContext = mock[RuntimeContext]

    //subtask 3
    when(runtimeContext.getIndexOfThisSubtask).thenReturn(2)

    //set runtime context
    operator.setRuntimeContext(runtimeContext)

    //await the clearing of the collection
    await(clearCollection(collectionName))

    //open operator
    operator.open(new Configuration())

    //setup mocking environment
    val mockFuture = mock[ResultFuture[Commit]]

    //argument captor
    val captor : ArgumentCaptor[java.util.Collection[Commit]] = ArgumentCaptor.forClass(classOf[java.util.Collection[Commit]])

    //reset token
    operator.GitHubAPI.client.setOAuth2Token("")

    //call invoke
    operator.asyncInvoke((repo, simpleCommit), mockFuture)

    //wait and capture
    verify(mockFuture, Mockito.timeout(10000)).complete(captor.capture())

    //verify that GetFunction is called
    verify(operator, Mockito.timeout(10000)).getFunction(any[(String, SimpleCommit)])

    //asser the correct is found
    assert(captor.getValue.asScala.head.sha == simpleCommit.sha)
  }



}
