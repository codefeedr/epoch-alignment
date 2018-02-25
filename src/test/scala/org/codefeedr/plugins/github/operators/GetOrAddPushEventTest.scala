package org.codefeedr.plugins.github.operators

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.codefeedr.plugins.github.clients.GitHubProtocol._
import org.codefeedr.core.LibraryServiceSpec
import org.mongodb.scala.Completed
import org.bson.conversions.Bson
import org.codefeedr.plugins.github.clients.MongoDB
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

class GetOrAddPushEventTest extends MongoGitHubSpec {

  val collectionName = "github_events"

  "The correct indexes" should "be set when the GetOrAddPushEvent is initialized" taggedAs(Slow) in async {
    val operator = new GetOrAddPushEvent()
    await(operator.setIndexes(operator.getIndexNames))

    val indexes = await {
        operator.
          mongoDB.
          getCollection(operator.getCollectionName).
          listIndexes().toFuture()
    }

    //find the correct index
    val findIndex = indexes.flatMap(_.find(x => x._1 == "key" && x._2.asDocument().containsKey("id")))
    assert(findIndex.size == 1)
  }

  "A PushEvent" should "be ignored if already in the DB" taggedAs (Slow) in async {
    val operator = new GetOrAddPushEvent()

    //get pushevent
    val pushEvent = fakePush()

    //await the clearing of the collection
    await(clearCollection(collectionName))

    operator.open(new Configuration()) //open operator

    //insert document
    await(insertDocument(collectionName, pushEvent))

    //setup mocking environment
    val mockFuture = mock[ResultFuture[PushEvent]]

    //call invoke
    operator.asyncInvoke(pushEvent, mockFuture)

    //verify the future has been used
    verify(mockFuture, timeout(1000).times(1)).complete(List().asJavaCollection)

    succeed
  }

  "A PushEvent" should "be stored and forwarded if not in the DB" taggedAs (Slow) in async {
    val operator = new GetOrAddPushEvent()

    //get pushevent
    val pushEvent = fakePush()

    //await the clearing of the collection
    await(clearCollection(collectionName))

    operator.open(new Configuration()) //open operator

    //setup mocking environment
    val mockFuture = mock[ResultFuture[PushEvent]]

    //not in db first
    val notInDB = await(mongo.getCollection(collectionName).find[PushEvent](Filters.equal("id", pushEvent.id)).toFuture())

    //check not in db
    assert(notInDB.size == 0)

    //call invoke
    operator.asyncInvoke(pushEvent, mockFuture)

    //sleep so it can be inserted
    Thread.sleep(2000)

    //verify the future has been used
    verify(mockFuture, timeout(1000).times(1)).complete(List(pushEvent).asJavaCollection)

    //verify it is in database
    val inDB = await(mongo.getCollection(collectionName).find[PushEvent](Filters.equal("id", pushEvent.id)).toFuture())

    //should be in db now
    assert(inDB.size == 1)
  }
}
