package org.codefeedr.Util

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{Assertion, Matchers}

import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{Await, Future, TimeoutException}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Some extensions to futures useful in the test project
  */
object FutureExtensions extends LazyLogging with Matchers {

  implicit class AssertableFuture[T](o: Future[T]) {
    /**
      *
      * @return
      */
    def AssertTimeout(): scala.concurrent.Future[Assertion] = AssertTimeout(Duration(100, MILLISECONDS))


    def AssertTimeout(d: Duration) = Future {
        assertThrows[TimeoutException](Await.ready(o, d))
    }
  }
}
