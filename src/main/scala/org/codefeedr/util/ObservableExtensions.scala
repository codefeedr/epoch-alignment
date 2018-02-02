package org.codefeedr.util

import com.typesafe.scalalogging.LazyLogging
import rx.lang.scala.{Observable, Subscription}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

object ObservableExtension extends LazyLogging {
  implicit class FutureObservable[T](o: Observable[T]) {

    /**
      * Returns a future that resolves to true when an event is recieved that matches the passed condition
      * If the observable completes, the future is succeeded with the value "None"
      * If the observable passes an error, the future is put into the failed state with the error from the observable
      * The creation of this future has a subscription on the observable has side effect
      * @param condition condition to watch for
      * @return
      */
    def SubscribeUntil(condition: T => Boolean): Future[Option[T]] = {
      val p = Promise[Option[T]]
      val subscription: Subscription = o.subscribe(
        (data: T) => {
          if (condition(data)) {
            p.success(Some(data))
          }
        },
        (e: Throwable) => p.failure(e),
        () => p.success(None)
      )
      //Make sure to unsubscribe when the future completes
      //In case of failure there is no need for this
      p.future.onComplete(_ => subscription.unsubscribe())
      p.future
    }

    /**
      * Subscribes on the observable, and returns a future that resolves when an error occurs in the observable
      * @return the future that resolves on the error
      */
    def AwaitError(): Future[Option[Throwable]] = {
      val p = Promise[Option[Throwable]]
      val subscription: Subscription = o.subscribe(
        (_: T) => Unit,
        (e: Throwable) => p.success(Some(e)),
        () => p.success(None)
      )
      //Make sure to unsubscribe when the future completes
      //In case of failure there is no need for this
      p.future.onComplete(_ => subscription.unsubscribe())
      p.future
    }

    /**
      * Collects all data until the stream ends
      * Note this future will never complete if the stream never ends, and keeps increasing in size
      * Meant for testing purposes
      * @return
      */
    def Collect(): Future[List[T]] = {
      val p = Promise[List[T]]
      val collection = new mutable.ListBuffer[T]
      val subscription = o.subscribe(element => {
        logger.debug("Got element")
        collection.append(element)
      }, e => p.failure(e), () => p.success(collection.toList))
      p.future
    }
  }
}
