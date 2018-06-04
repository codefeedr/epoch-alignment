package org.codefeedr.util

import org.codefeedr.core.library.internal.zookeeper.ZkNodeBase
import org.mockito.Mockito.when
import org.mockito.{ArgumentMatcher, ArgumentMatchers}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.AsyncFlatSpec
import org.scalatest.mockito.MockitoSugar


import scala.concurrent.{Future, Promise}

trait MockitoExtensions {

    def answer[T](f: InvocationOnMock => T): Answer[T] = {
      //Ignore the warning, compiler needs it
      new Answer[T] {
        override def answer(invocation: InvocationOnMock): T = f(invocation)
      }
    }

  def awaitAndReturn[T](p:Promise[Unit], a: T,times:Int) = awaitAndAnswer(p,_=>a,times)

  def awaitAndAnswer[T](p:Promise[Unit], f: InvocationOnMock => T,times:Int): Answer[T] = {
    var i = times

    new Answer[T] {
      override def answer(invocation: InvocationOnMock): T = {
        i = i-1
        if(i == 0) {
          p.success(())
        }

        if(i <= 0) {
          //Hack: Free up thread in case of running single threaded..
          //Hack: Only used from tests
          Thread.sleep(10)
        }
        f(invocation)
      }
    }
  }

  def Matches[T](matcher: T => Boolean): T = {
    ArgumentMatchers.argThat(
    new ArgumentMatcher[T] {
      override def matches(argument: T): Boolean = matcher(argument)
    })
  }


  /**
    * Mocks the lock on the given node
    * @param zkNodeBase
    */
  def mockLock(zkNodeBase: ZkNodeBase): Unit = {
    when(zkNodeBase.asyncWriteLock(ArgumentMatchers.any[() => Future[Unit]]()))
      .thenAnswer(answer(a => a.getArgument[() => Future[Unit]](0)()))

  }

}