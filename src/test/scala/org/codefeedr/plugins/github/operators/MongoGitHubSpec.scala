package org.codefeedr.plugins.github.operators

import java.time.LocalDate
import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.core.LibraryServiceSpec
import org.codefeedr.plugins.github.clients.GitHubProtocol._
import org.codefeedr.plugins.github.clients.MongoDB
import org.mongodb.scala.Completed
import org.scalatest.Matchers
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.Future
import scala.reflect.ClassTag

class MongoGitHubSpec extends LibraryServiceSpec with Matchers with MockitoSugar with LazyLogging {

  val mongo = new MongoDB()

  def fakeCommit(date : String = "") = {
    Commit("2439402a43e11b5efa2a680ac31207f2210b63d5",
      "https://api.github.com/repos/codefeedr/codefeedr/commits/2439402a43e11b5efa2a680ac31207f2210b63d5",
      CommitData(
        CommitUser("wouter", "test", ""),
        CommitUser("wouter", "test", ""),
        "test",
        Tree("test"),
        1,
        Verification(false, "", None, None)),
      Some(User(1, "wouter", "test", "test", false)),
      Some(User(1, "wouter", "test", "test", false)),
      Nil,
      Stats(2, 1, 1),
      Nil)
  }

  def fakePush() = {
    PushEvent("123",
      Repo(123, "codefeedr/codefeedr"),
      Actor(123, "test", "test", "test"),
      None,
      Payload(123, 0, 0, "testRef", "5f2bd246c8245d83dfc770c989b8879d47e55b1c", "doesntMatter", Nil),
      true,
      new Date())
  }

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
