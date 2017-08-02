package org.codefeedr.Engine.Query

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.Library.Internal.SubjectTypeFactory
import org.codefeedr.Model.{PropertyType, SubjectType}
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, FlatSpec, Matchers}

case class SomeJoinTestObject(id: Int, name: String)
case class SomeJoinTestMessage(id: Int, objectId: Int, message: String, dataBag: Array[Byte])

/**
  * Created by Niels on 02/08/2017.
  */
class JoinQueryComposerSpec
    extends AsyncFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with LazyLogging {

  val objectType: SubjectType = SubjectTypeFactory.getSubjectType[SomeJoinTestObject](Array("id"))
  val messageType: SubjectType = SubjectTypeFactory.getSubjectType[SomeJoinTestMessage](Array("id"))

  "A buildComposedType method" should "create a new type with the given alias" in {
    val mergedType = JoinQueryComposer.buildComposedType(objectType,
                                                         messageType,
                                                         Array("name"),
                                                         Array("message"),
                                                         "testObjectMessages")
    assert(mergedType.name == "testObjectMessages")
  }

  "A buildComposedType method" should "merge properties from left and right" in {
    val mergedType = JoinQueryComposer.buildComposedType(objectType,
                                                         messageType,
                                                         Array("name"),
                                                         Array("message"),
                                                         "testObjectMessages")

    assert(mergedType.properties.length == 2)
    assert(mergedType.properties.map(o => o.name).contains("name"))
    assert(mergedType.properties.map(o => o.name).contains("message"))

  }

  "A buildComposedType method" should "copy type information from source types" in {
    val mergedType = JoinQueryComposer.buildComposedType(objectType,
                                                         messageType,
                                                         Array("name"),
                                                         Array("id", "message", "dataBag"),
                                                         "testObjectMessages")
    assert(mergedType.properties.length == 4)
    assert(
      mergedType.properties.filter(o => o.name == "name").head.propertyType == PropertyType.String)
    assert(
      mergedType.properties.filter(o => o.name == "id").head.propertyType == PropertyType.Number)
    assert(
      mergedType.properties
        .filter(o => o.name == "message")
        .head
        .propertyType == PropertyType.String)
    assert(
      mergedType.properties.filter(o => o.name == "dataBag").head.propertyType == PropertyType.Any)
  }

  "A buildComposedType method" should "throw an exception when a name occurs twice in the select" in {
    assertThrows[Exception](
      JoinQueryComposer.buildComposedType(objectType,
                                          messageType,
                                          Array("id", "name"),
                                          Array("id", "message", "dataBag"),
                                          "testObjectMessages"))
  }

}
