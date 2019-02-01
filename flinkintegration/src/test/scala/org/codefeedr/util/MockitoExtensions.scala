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


object MockitoExtensions {
  implicit class answerp1[P1, T](factory: P1 => T) extends Answer[T] {
    override def answer(invocation: InvocationOnMock): T = factory(invocation.getArgument[P1](0))
  }

  implicit class answerp2[P1,P2, T](factory: (P1,P2) => T) extends Answer[T] {
    override def answer(invocation: InvocationOnMock): T = factory(invocation.getArgument[P1](0),invocation.getArgument[P2](1))
  }
}