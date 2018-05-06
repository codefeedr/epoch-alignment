package org.codefeedr.util

import org.mockito.{ArgumentMatcher, ArgumentMatchers}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.AsyncFlatSpec
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.Promise

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
          p.success()
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

}