package org.codefeedr.plugins.github.generate

import org.scalatest.FlatSpec

class UserGeneratorSpec extends FlatSpec {
  "UserGenerator" should "Generate the same element with the same seed" in {
    assert(new UserGeneratorSource(10).generate() != new UserGeneratorSource(10).generate())
  }

  it should "Generate different elements with different seeds" in {
    assert(new UserGeneratorSource(10).generate() != new UserGeneratorSource(11).generate())
  }
}
