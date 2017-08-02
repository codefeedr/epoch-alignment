package org.codefeedr.Engine.Query

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.Library.Internal.{RecordTransformer, SubjectTypeFactory}
import org.codefeedr.Model.{ActionType, _}
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

  "A MergeFunction" should "map properties from two types into a single type based on given fieldnames, and compose the source trail" in {

    val mergedType: SubjectType =
      JoinQueryComposer.buildComposedType(objectType,
                                          messageType,
                                          Array("name"),
                                          Array("id", "message", "dataBag"),
                                          "testObjectMessages")

    val mergeFn: (TrailedRecord, TrailedRecord, ActionType.Value) => TrailedRecord =
      JoinQueryComposer.buildMergeFunction(objectType,
                                           messageType,
                                           mergedType,
                                           Array("name"),
                                           Array("id", "message", "dataBag"),
                                           Array[Byte](12.toByte))

    val o = TrailedRecord(objectTransformer.Bag(SomeJoinTestObject(1, "object 1"), ActionType.Add),
                          Source(Array[Byte](), Array[Byte](10.toByte)))
    val m =
      TrailedRecord(
        messageTransformer.Bag(SomeJoinTestMessage(2, 3, "a message", Array[Byte](4.toByte)),
                               ActionType.Add),
        Source(Array[Byte](), Array[Byte](11.toByte)))

    val merged = mergeFn(o, m, ActionType.Add)
    assert(merged.record.data(0).asInstanceOf[String].equals("object 1"))
    assert(merged.record.data(1).asInstanceOf[Int] == 2)
    assert(merged.record.data(2).asInstanceOf[String].equals("a message"))
    assert(merged.record.data(3).asInstanceOf[Array[Byte]](0) == 4.toByte)
    assert(merged.record.action == ActionType.Add)
    assert(merged.trail.isInstanceOf[ComposedSource])
    assert(merged.trail.asInstanceOf[ComposedSource].SourceId(0) == 12.toByte)
    assert(
      merged.trail
        .asInstanceOf[ComposedSource]
        .pointers(0)
        .asInstanceOf[Source]
        .Key(0) == 10.toByte)
    assert(
      merged.trail
        .asInstanceOf[ComposedSource]
        .pointers(1)
        .asInstanceOf[Source]
        .Key(0) == 11.toByte)
  }

  val mergedType: SubjectType =
    JoinQueryComposer.buildComposedType(objectType,
                                        messageType,
                                        Array("name"),
                                        Array("id", "message", "dataBag"),
                                        "testObjectMessages")

  val mergeFn: (TrailedRecord, TrailedRecord, ActionType.Value) => TrailedRecord =
    JoinQueryComposer.buildMergeFunction(objectType,
                                         messageType,
                                         mergedType,
                                         Array("name"),
                                         Array("id", "message", "dataBag"),
                                         Array[Byte](12.toByte))

  "A InnerJoinFunction" should "not emit values until a join partner has been found" in {
    val state = None: Option[JoinState]
    val joinFunction = JoinQueryComposer.mapSideInnerJoin(mergeFn) _
    val o = TrailedRecord(objectTransformer.Bag(SomeJoinTestObject(1, "object 1"), ActionType.Add),
                          Source(Array[Byte](), Array[Byte](10.toByte)))

    val m =
      TrailedRecord(
        messageTransformer.Bag(SomeJoinTestMessage(2, 3, "a message", Array[Byte](4.toByte)),
                               ActionType.Add),
        Source(Array[Byte](), Array[Byte](11.toByte)))

    val (r1, state1) = joinFunction(Left(o), state)
    assert(r1.isEmpty)
    assert(state1.get.left.values.toArray.contains(o.record))

    val (r2, state2) = joinFunction(Right(m), state)
    assert(r2.isEmpty)
    assert(state2.get.right.values.toArray.contains(m.record))
  }

  "A InnerJoinFunction" should "emit a value when a join partner has been found" in {
    val state = None: Option[JoinState]
    val joinFunction = JoinQueryComposer.mapSideInnerJoin(mergeFn) _
    val o = TrailedRecord(objectTransformer.Bag(SomeJoinTestObject(1, "object 1"), ActionType.Add),
                          Source(Array[Byte](), Array[Byte](10.toByte)))

    val m =
      TrailedRecord(
        messageTransformer.Bag(SomeJoinTestMessage(2, 3, "a message", Array[Byte](4.toByte)),
                               ActionType.Add),
        Source(Array[Byte](), Array[Byte](11.toByte)))

    val (r1, state1) = joinFunction(Left(o), state)
    assert(r1.isEmpty)
    assert(state1.get.left.values.toArray.contains(o.record))

    val (r2, state2) = joinFunction(Right(m), state1)
    assert(r2.size == 1)
    assert(state2.get.right.values.toArray.contains(m.record))
    assert(state2.get.left.values.toArray.contains(o.record))
  }

  "A InnerJoinFunction" should "emit a cross join when multiple join partners are found in" in {
    val state = None: Option[JoinState]
    val joinFunction = JoinQueryComposer.mapSideInnerJoin(mergeFn) _
    val o = TrailedRecord(objectTransformer.Bag(SomeJoinTestObject(1, "object 1"), ActionType.Add),
                          Source(Array[Byte](), Array[Byte](10.toByte)))

    val o2 =
      TrailedRecord(objectTransformer.Bag(SomeJoinTestObject(1, "Another object"), ActionType.Add),
                    Source(Array[Byte](), Array[Byte](10.toByte)))

    val m =
      TrailedRecord(
        messageTransformer.Bag(SomeJoinTestMessage(2, 3, "a message", Array[Byte](4.toByte)),
                               ActionType.Add),
        Source(Array[Byte](), Array[Byte](11.toByte)))
    val m2 =
      TrailedRecord(
        messageTransformer.Bag(SomeJoinTestMessage(2, 3, "another message", Array[Byte](4.toByte)),
                               ActionType.Add),
        Source(Array[Byte](), Array[Byte](11.toByte)))

    val (r1, state1) = joinFunction(Left(o), state)
    val (r2, state2) = joinFunction(Right(m), state1)
    val (r3, state3) = joinFunction(Left(o2), state2)
    assert(r3.size == 1)
    assert(state3.get.right.values.toArray.contains(m.record))
    assert(state3.get.left.values.toArray.contains(o.record))
    assert(state3.get.left.values.toArray.contains(o2.record))
    val (r4, state4) = joinFunction(Right(m2), state3)
    assert(r4.size == 2)
    assert(state4.get.right.values.toArray.contains(m.record))
    assert(state4.get.right.values.toArray.contains(m2.record))
    assert(state4.get.left.values.toArray.contains(o.record))
    assert(state4.get.left.values.toArray.contains(o2.record))
  }
  "A InnerJoinFunction" should "not add an element to its state twice if it occurs twice" in {
    val state = None: Option[JoinState]
    val joinFunction = JoinQueryComposer.mapSideInnerJoin(mergeFn) _
    val o = TrailedRecord(objectTransformer.Bag(SomeJoinTestObject(1, "object 1"), ActionType.Add),
                          Source(Array[Byte](), Array[Byte](10.toByte)))

    val o2 =
      TrailedRecord(objectTransformer.Bag(SomeJoinTestObject(1, "Another object"), ActionType.Add),
                    Source(Array[Byte](), Array[Byte](10.toByte)))

    val m =
      TrailedRecord(
        messageTransformer.Bag(SomeJoinTestMessage(2, 3, "a message", Array[Byte](4.toByte)),
                               ActionType.Add),
        Source(Array[Byte](), Array[Byte](11.toByte)))
    val m2 =
      TrailedRecord(
        messageTransformer.Bag(SomeJoinTestMessage(2, 3, "another message", Array[Byte](4.toByte)),
                               ActionType.Add),
        Source(Array[Byte](), Array[Byte](11.toByte)))

    val (r1, state1) = joinFunction(Left(o), state)
    val (r2, state2) = joinFunction(Right(m), state1)
    val (r3, state3) = joinFunction(Left(o), state2)
    assert(r3.size == 1)
    assert(state3.get.right.values.toArray.count(l => l.equals(m.record)) == 1)
    assert(state3.get.left.values.toArray.count(l => l.equals(o.record)) == 1)
    val (r4, state4) = joinFunction(Right(m), state3)
    assert(r4.size == 1)
    assert(state3.get.right.values.toArray.count(l => l.equals(m.record)) == 1)
    assert(state3.get.left.values.toArray.count(l => l.equals(o.record)) == 1)
  }
}
