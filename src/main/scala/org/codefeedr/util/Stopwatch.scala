package org.codefeedr.util

import scala.concurrent.duration._

class Stopwatch {
  private var startedAtNanos = System.nanoTime()

  /**
    * @return elapsed time since object creation
    */
  def elapsed() :Duration = {
    (System.nanoTime() - startedAtNanos).nanos
  }

  /**
    * Resets the stopwatch
    */
  def reset(): Unit = {
    startedAtNanos = System.nanoTime()
  }
}

object Stopwatch {
  def start(): Stopwatch = new Stopwatch
}