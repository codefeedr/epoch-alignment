package org.codefeedr.util

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{Assertion, Matchers}

import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{Await, Future, TimeoutException}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Some extensions to futures useful in the test project
  */
object futureExtensions extends LazyLogging with Matchers {

  implicit class AssertableFuture[T](o: Future[T]) {
    /**
      *
      * @return
      */
    def assertTimeout(): scala.concurrent.Future[Assertion] = assertTimeout(Duration(100, MILLISECONDS))


    def assertTimeout(d: Duration) = Future {
        assertThrows[TimeoutException](Await.ready(o, d))
    }
  }
}
