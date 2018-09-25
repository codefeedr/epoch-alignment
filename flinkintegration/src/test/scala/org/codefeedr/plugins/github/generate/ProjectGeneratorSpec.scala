package org.codefeedr.plugins.github.generate

import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.FlatSpec

class ProjectGeneratorSpec extends FlatSpec {
  implicit val eventTime: DateTime = DateTime.now(DateTimeZone.UTC)

  "ProjectGenerator" should "Generate the same element with the same seed" in {
    assert(new ProjectGenerator(10,Some(eventTime)).generate(1) == new ProjectGenerator(10,Some(eventTime)).generate(1))
  }

  it should "Generate different elements with different seeds" in {
    assert(new ProjectGenerator(10,Some(eventTime)).generate(1) != new ProjectGenerator(11,Some(eventTime)).generate(1))
  }
}
