package org.codefeedr.Core.Operators

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.Core.Clients.MongoDB.MongoDB
import org.codefeedr.Core.LibraryServiceSpec
import org.mongodb.scala.Completed
import org.scalatest.Matchers
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.Future
import scala.reflect.ClassTag

class MongoDBSpec extends LibraryServiceSpec with Matchers with MockitoSugar with LazyLogging {

  val mongo = new MongoDB()

  def clearCollection(collectionName: String): Future[Completed] = {
    mongo.
      getCollection(collectionName).
      drop().
      toFuture()
  }

  def insertDocument[T : ClassTag](collectionName: String, doc: T) = {
    mongo.
      getCollection[T](collectionName).
      insertOne(doc).
      toFuture()
  }

}
