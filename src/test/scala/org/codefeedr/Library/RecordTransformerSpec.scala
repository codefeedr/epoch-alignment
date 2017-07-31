package org.codefeedr.Library

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.Library.Internal.{RecordTransformer, RecordUtils, SubjectTypeFactory}
import org.codefeedr.Model.ActionType
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, Matchers}

case class BagTestInt(intValue: Int)
case class BagTestString(stringValue: String)
case class BagTestObject(objectValue: Object)
class BagTestPojo(var intValue: Int)

/**
  * Created by Niels on 28/07/2017.
  */
class RecordTransformerSpec
    extends AsyncFlatSpec
    with Matchers
    with LazyLogging
    with BeforeAndAfterAll {


  override def afterAll() {
    SubjectLibrary.UnRegisterSubject("BagTestInt")
    SubjectLibrary.UnRegisterSubject("BagTestString")
    SubjectLibrary.UnRegisterSubject("BagTestObject")
    SubjectLibrary.UnRegisterSubject("BagTestPojo")
  }

  "A RecordTransformer" should "be able to map integers to a bag" in {
    val t = SubjectTypeFactory.getSubjectType[BagTestInt]
    val bagger = new RecordTransformer[BagTestInt](t)
    val obj = BagTestInt(5)
    implicit val record = bagger.Bag(obj, ActionType.Add)
    assert(new RecordUtils(t).getValueT[Int]("intValue") == 5)
  }

  "A RecordTransformer" should "be able to map strings to a bag" in {
    val t = SubjectTypeFactory.getSubjectType[BagTestString]
    val bagger = new RecordTransformer[BagTestString](t)
    val obj = BagTestString("someString")
    implicit val record = bagger.Bag(obj, ActionType.Add)
    assert(new RecordUtils(t).getValueT[String]("stringValue") == "someString")
  }


  "A RecordTransformer" should "be able to map any object to a bag" in {
    val t = SubjectTypeFactory.getSubjectType[BagTestObject]
    val bagger = new RecordTransformer[BagTestObject](t)
    val obj = BagTestObject(Seq(2, 3, 4))
    implicit val record = bagger.Bag(obj, ActionType.Add)
    assert(new RecordUtils(t).getValueT[Seq[Int]]("objectValue").size == 3)
  }
  "A RecordTransformer" should "be able to map pojo" in {
    val t = SubjectTypeFactory.getSubjectType[BagTestPojo]
    val bagger = new RecordTransformer[BagTestPojo](t)
    val obj = new BagTestPojo(2)
    implicit val record = bagger.Bag(obj, ActionType.Add)
    assert(new RecordUtils(t).getValueT[Int]("intValue") == 2)
  }

  "A RecordTransformer" should "be able to map integers from a bag to object" in {
    val t = SubjectTypeFactory.getSubjectType[BagTestInt]
    val bagger = new RecordTransformer[BagTestInt](t)
    val obj = BagTestInt(5)
    implicit val record = bagger.Bag(obj, ActionType.Add)
    val r = bagger.Unbag(record)
    assert(r.intValue == 5)
  }
  "A RecordTransformer" should "be able to map strings from a bag to object" in {
    val t = SubjectTypeFactory.getSubjectType[BagTestString]
    val bagger = new RecordTransformer[BagTestString](t)
    val obj = BagTestString("someString")
    implicit val record = bagger.Bag(obj, ActionType.Add)
    val r = bagger.Unbag(record)
    assert(r.stringValue == "someString")
  }

  "A RecordTransformer" should "be able to map any object from a bag to object" in {
    val t = SubjectTypeFactory.getSubjectType[BagTestObject]
    val bagger = new RecordTransformer[BagTestObject](t)
    val obj = BagTestObject(Seq(2, 3, 4))
    implicit val record = bagger.Bag(obj, ActionType.Add)
    val r = bagger.Unbag(record)
    assert(r.objectValue.asInstanceOf[Seq[Int]].size == 3)
  }
  "A RecordTransformer" should "be able to map a bag back to pojo" in {
    val t = SubjectTypeFactory.getSubjectType[BagTestPojo]
    val bagger = new RecordTransformer[BagTestPojo](t)
    val obj = new BagTestPojo(2)
    implicit val record = bagger.Bag(obj, ActionType.Add)
    val r = bagger.Unbag(record)
    assert(r.intValue == 2)
  }

}
