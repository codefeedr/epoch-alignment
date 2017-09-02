

package org.codefeedr.Core.Library.Internal

import org.codefeedr.Model.{PropertyType, SubjectType}
import org.scalatest.{AsyncFlatSpec, FlatSpec, Matchers}

class A {
  val i: Int = 2
}
class B {
  val s: String = "hallo"
}
class C {
  //Just some random object
  val o: List[B] = List[B]()
}
class D {
  val i: Int = 2
  val s: String = "hallo"
  val o: List[B] = List[B]()
}

case class E(i: Int, o: List[B], s: String)

class F {
  val i: Int = 2
  def s: String = "hallo"
  def o: List[B] = List[B]()
}

/**
  * Created by Niels on 14/07/2017.
  */
class SubjectTypeFactorySpec extends FlatSpec with Matchers {
  "A SubjectTypeFactory" should "Create a new type with int properties" in {
    val t: SubjectType = SubjectTypeFactory.getSubjectType[A]
    assert(t.name == "A")
    assert(t.properties.length == 1)
    for (p <- t.properties.filter(o => o.name == "i")) {
      assert(p.propertyType == PropertyType.Number)
    }
  }

  "A SubjectTypeFactory" should "Create a new type with string properties" in {
    val t: SubjectType = SubjectTypeFactory.getSubjectType[B]
    assert(t.name == "B")
    assert(t.properties.length == 1)
    for (p <- t.properties.filter(o => o.name == "s")) {
      assert(p.propertyType == PropertyType.String)
    }
  }

  "A SubjectTypeFactory" should "Use any for unknown objects" in {
    val t: SubjectType = SubjectTypeFactory.getSubjectType[C]
    assert(t.name == "C")
    assert(t.properties.length == 1)
    for (p <- t.properties.filter(o => o.name == "o")) {
      assert(p.propertyType == PropertyType.Any)
    }
  }

  "A SubjectTypeFactory" should " support multiple properties" in {
    val t: SubjectType = SubjectTypeFactory.getSubjectType[D]
    assert(t.name == "D")
    assert(t.properties.length == 3)
    for (p <- t.properties) {
      p.name match {
        case "o" => assert(p.propertyType == PropertyType.Any)
        case "i" => assert(p.propertyType == PropertyType.Number)
        case "s" => assert(p.propertyType == PropertyType.String)
        case _   => assert(true)
      }
    }
  }

  "A SubjectTypeFactory" should " support case classes " in {
    val t: SubjectType = SubjectTypeFactory.getSubjectType[E]
    assert(t.name == "E")
    assert(t.properties.length == 3)
    for (p <- t.properties) {
      p.name match {
        case "o" => assert(p.propertyType == PropertyType.Any)
        case "i" => assert(p.propertyType == PropertyType.Number)
        case "s" => assert(p.propertyType == PropertyType.String)
        case _   => assert(true)
      }
    }
  }

  "A SubjectTypeFactory" should " ignore definitions " in {
    val t: SubjectType = SubjectTypeFactory.getSubjectType[F]
    assert(t.name == "F")
    assert(t.properties.length == 1)
    for (p <- t.properties) {
      p.name match {
        case "o" => assert(p.propertyType == PropertyType.Any)
        case "i" => assert(p.propertyType == PropertyType.Number)
        case "s" => assert(p.propertyType == PropertyType.String)
        case _   => assert(true)
      }
    }
  }

  "A SubjectTypeFactory" should " expose keyfields given as parameter" in {
    val t = SubjectTypeFactory.getSubjectType[D](Array("i", "s"))
    assert(t.properties.filter(o => o.name == "i").count(o => o.id) == 1)
    assert(t.properties.filter(o => o.name == "s").count(o => o.id) == 1)
    assert(t.properties.filter(o => o.name == "o").count(o => o.id) == 0)
  }

  "A SubjectTypeFactory" should " throw an exception when keyfields do not exist" in {
    assertThrows[Exception](SubjectTypeFactory.getSubjectType[D](Array("A")))
  }
}
