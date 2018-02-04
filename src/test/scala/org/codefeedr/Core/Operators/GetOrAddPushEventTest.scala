package org.codefeedr.Core.Operators

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.codefeedr.Core.Clients.GitHub.GitHubProtocol.{Actor, Payload, PushEvent, Repo}
import org.codefeedr.Core.Clients.MongoDB.MongoDB
import org.codefeedr.Core.LibraryServiceSpec
import org.mongodb.scala.Completed
import org.bson.conversions.Bson
import org.scalatest.mockito.MockitoSugar
import org.scalatest.tagobjects.Slow
import org.scalatest.{AsyncFlatSpec, FlatSpec, Matchers}
import org.mockito.Mockito._
import org.mongodb.scala.model.Filters

import scala.async.Async.{async, await}
import scala.concurrent._
import ExecutionContext.Implicits.global
import collection.JavaConverters._
import scala.reflect.ClassTag

class GetOrAddPushEventTest extends MongoDBSpec {

  val collectionName = "github_events"

  val fakePush = PushEvent("123",
    Repo(123, "test/test"),
    Actor(123, "test", "test", "test"),
    None,
    Payload(123, 1, 1, "testRef", "testHead", "testBefore", Nil),
    true,
    new Date())

  "The correct indexes" should "be set when the GetOrAddPushEvent is initialized" taggedAs(Slow) in async {
    val operator = new GetOrAddPushEvent()
    await(operator.SetIndexes(operator.GetIndexNames))

    val indexes = await {
        operator.
          mongoDB.
          getCollection(operator.GetCollectionName).
          listIndexes().toFuture()
    }

    //find the correct index
    val findIndex = indexes.flatMap(_.find(x => x._1 == "key" && x._2.asDocument().containsKey("id")))
    assert(findIndex.size == 1)
  }

  "A PushEvent" should "be forwarded if already in the DB" taggedAs (Slow) in async {
    val operator = new GetOrAddPushEvent()
    operator.open(new Configuration()) //open operator

    //await the clearing of the collection
    await(ClearCollection(collectionName))
    await(InsertDocument(collectionName, fakePush))

    //setup mocking environment
    val mockFuture = mock[ResultFuture[PushEvent]]

    //call invoke
    operator.asyncInvoke(fakePush, mockFuture)

    //verify the future has been used
    verify(mockFuture, timeout(1000).times(1)).complete(List(fakePush).asJavaCollection)

    succeed
  }

  "A PushEvent" should "be stored and forwarded if not in the DB" taggedAs (Slow) in async {
    val operator = new GetOrAddPushEvent()
    operator.open(new Configuration()) //open operator

    //await the clearing of the collection
    await(ClearCollection(collectionName))

    //setup mocking environment
    val mockFuture = mock[ResultFuture[PushEvent]]

    //not in db first
    val notInDB = await(mongo.getCollection(collectionName).find[PushEvent](Filters.equal("id", fakePush.id)).toFuture())

    //check not in db
    assert(notInDB.size == 0)

    //call invoke
    operator.asyncInvoke(fakePush, mockFuture)

    //verify the future has been used
    verify(mockFuture, timeout(1000).times(1)).complete(List(fakePush).asJavaCollection)

    //verify it is in database
    val inDB = await(mongo.getCollection(collectionName).find[PushEvent](Filters.equal("id", fakePush.id)).toFuture())

    //should be in db now
    assert(inDB.size == 1)
  }

}
