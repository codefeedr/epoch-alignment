package org.codefeedr.plugins.github.generate

import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.FlatSpec

class CommitGeneratorSpec extends FlatSpec {
  val eventTime: Long = System.currentTimeMillis()

  "CommitGenerator" should "Generate the same element with the same seed" in {
    assert(new CommitGenerator(10,0,0,Some(eventTime)).generate() == new CommitGenerator(10,0,0,Some(eventTime)).generate())
  }

  it should "Generate different elements with different seeds" in {
    assert(new CommitGenerator(10,0,0,Some(eventTime)).generate() != new CommitGenerator(11,0,0,Some(eventTime)).generate())
  }
}
