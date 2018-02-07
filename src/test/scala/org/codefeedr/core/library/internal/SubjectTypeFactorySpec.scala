

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.codefeedr.core.Library.Internal

import org.apache.flink.streaming.api.scala.createTypeInformation
import org.codefeedr.Model.SubjectType
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
}

case class E(i: Int, s: String)

class F {
  val i: Int = 2
  def s: String = "hallo"
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
      assert(p.propertyType.canEqual(createTypeInformation[Int]))
    }
  }

  "A SubjectTypeFactory" should "Create a new type with string properties" in {
    val t: SubjectType = SubjectTypeFactory.getSubjectType[B]
    assert(t.name == "B")
    assert(t.properties.length == 1)
    for (p <- t.properties.filter(o => o.name == "s")) {
      assert(p.propertyType.canEqual(createTypeInformation[String]))
    }
  }

  //TODO: Think of a way around this
  //Currently The SubjectTypeFactory maps types to its java equivalent before calling Flink, because Flink's public API forces the use of generics and macros.
  //This means scala collections lose their generic type information in subject type
  /*
  "A SubjectTypeFactory" should "Support more complex objects" in {
    val t: SubjectType = SubjectTypeFactory.getSubjectType[C]
    assert(t.name == "C")
    assert(t.properties.length == 1)
    val expected = createTypeInformation[List[]]
    for (p <- t.properties.filter(o => o.name == "o")) {
      assert(expected.canEqual(p.propertyType))
    }
  }
  */

  "A SubjectTypeFactory" should " support multiple properties" in {
    val t: SubjectType = SubjectTypeFactory.getSubjectType[D]
    assert(t.name == "D")
    assert(t.properties.length == 2)
    for (p <- t.properties) {
      p.name match {
        case "i" => assert(p.propertyType.canEqual(createTypeInformation[Int]))
        case "s" => assert(p.propertyType.canEqual(createTypeInformation[String]))
        case _   => assert(true)
      }
    }
  }

  "A SubjectTypeFactory" should " support case classes " in {
    val t: SubjectType = SubjectTypeFactory.getSubjectType[E]
    assert(t.name == "E")
    assert(t.properties.length == 2)
    for (p <- t.properties) {
      p.name match {
        case "i" => assert(p.propertyType.canEqual(createTypeInformation[Int]))
        case "s" => assert(p.propertyType.canEqual(createTypeInformation[String]))
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
        case "i" => assert(p.propertyType.canEqual(createTypeInformation[Int]))
        case "s" => assert(p.propertyType.canEqual(createTypeInformation[String]))
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

  "A SubjectTypeFactory" should " produce the same subjectType if some other subject types fields and properties are passed as parameter" in {
    val t: SubjectType = SubjectTypeFactory.getSubjectType[F]
    val fromTable = SubjectTypeFactory.getSubjectType("TestSubject", t.properties.map(o => o.name), t.properties.map(o => o.propertyType))
    fromTable.properties.shouldEqual(t.properties)
  }
}
