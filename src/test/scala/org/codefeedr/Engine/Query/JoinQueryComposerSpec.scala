package org.codefeedr.Engine.Query

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.Library.Internal.{RecordTransformer, SubjectTypeFactory}
import org.codefeedr.Model._
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

  val objectTransformer = new RecordTransformer[SomeJoinTestObject](objectType)
  val messageTransformer = new RecordTransformer[SomeJoinTestMessage](messageType)

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

  "A PartialKeyFunction" should "Produce equal key when the key values are equal" in {
    val keyFunction =
      JoinQueryComposer.buildPartialKeyFunction(Array("objectId", "message"), messageType)
    val m1 =
      TrailedRecord(messageTransformer.Bag(SomeJoinTestMessage(1, 1, "a message", Array[Byte]()),
                                           ActionType.Add),
                    Source(Array[Byte](), Array[Byte]()))
    val m2 =
      TrailedRecord(messageTransformer.Bag(SomeJoinTestMessage(2, 1, "a message", Array[Byte]()),
                                           ActionType.Add),
                    Source(Array[Byte](), Array[Byte]()))
    assert(keyFunction(m1).sameElements(keyFunction(m2)))
  }
  "A PartialKeyFunction" should "Produce different keys when the key values are not equal" in {
    val keyFunction =
      JoinQueryComposer.buildPartialKeyFunction(Array("objectId", "message"), messageType)
    val m1 =
      TrailedRecord(messageTransformer.Bag(SomeJoinTestMessage(1, 1, "a message", Array[Byte]()),
                                           ActionType.Add),
                    Source(Array[Byte](), Array[Byte]()))
    val m2 =
      TrailedRecord(
        messageTransformer.Bag(SomeJoinTestMessage(2, 1, "another message", Array[Byte]()),
                               ActionType.Add),
        Source(Array[Byte](), Array[Byte]()))
    assert(!keyFunction(m1).sameElements(keyFunction(m2)))
  }
}
