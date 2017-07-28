package org.codefeedr.Library

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.Library.Internal.{RecordTransformer, RecordUtils}
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
    SubjectLibrary
      .GetType[BagTestInt]()
      .map(t => {
        val bagger = new RecordTransformer[BagTestInt]()
        val obj = BagTestInt(5)
        implicit val record = bagger.Bag(obj, ActionType.Add)
        assert(RecordUtils.getValueT[Int]("intValue") == 5)
      })
  }
  "A RecordTransformer" should "be able to map strings to a bag" in {
    SubjectLibrary
      .GetType[BagTestString]()
      .map(t => {
        val bagger = new RecordTransformer[BagTestString]()
        val obj = BagTestString("someString")
        implicit val record = bagger.Bag(obj, ActionType.Add)
        assert(RecordUtils.getValueT[String]("stringValue") == "someString")
      })
  }
  "A RecordTransformer" should "be able to map any object to a bag" in {
    SubjectLibrary
      .GetType[BagTestObject]()
      .map(t => {
        val bagger = new RecordTransformer[BagTestObject]()
        val obj = BagTestObject(Seq(2, 3, 4))
        implicit val record = bagger.Bag(obj, ActionType.Add)
        assert(RecordUtils.getValueT[Seq[Int]]("objectValue").size == 3)
      })
  }
  "A RecordTransformer" should "be able to map pojo" in {
    SubjectLibrary
      .GetType[BagTestPojo]()
      .map(t => {
        val bagger = new RecordTransformer[BagTestPojo]()
        val obj = new BagTestPojo(2)
        implicit val record = bagger.Bag(obj, ActionType.Add)
        assert(RecordUtils.getValueT[Int]("intValue") == 2)
      })
  }

  "A RecordTransformer" should "be able to map integers from a bag to object" in {
    SubjectLibrary
      .GetType[BagTestInt]()
      .map(t => {
        val bagger = new RecordTransformer[BagTestInt]()
        val obj = BagTestInt(5)
        implicit val record = bagger.Bag(obj, ActionType.Add)
        val r = bagger.Unbag(record)
        assert(r.intValue == 5)
      })
  }
  "A RecordTransformer" should "be able to map strings from a bag to object" in {
    SubjectLibrary
      .GetType[BagTestString]()
      .map(t => {
        val bagger = new RecordTransformer[BagTestString]()
        val obj = BagTestString("someString")
        implicit val record = bagger.Bag(obj, ActionType.Add)
        val r = bagger.Unbag(record)
        assert(r.stringValue == "someString")
      })
  }
  "A RecordTransformer" should "be able to map any object from a bag to object" in {
    SubjectLibrary
      .GetType[BagTestObject]()
      .map(t => {
        val bagger = new RecordTransformer[BagTestObject]()
        val obj = BagTestObject(Seq(2, 3, 4))
        implicit val record = bagger.Bag(obj, ActionType.Add)
        val r = bagger.Unbag(record)
        assert(r.objectValue.asInstanceOf[Seq[Int]].size == 3)
      })
  }
  "A RecordTransformer" should "be able to map a bag back to pojo" in {
    SubjectLibrary
      .GetType[BagTestPojo]()
      .map(t => {
        val bagger = new RecordTransformer[BagTestPojo]()
        val obj = new BagTestPojo(2)
        implicit val record = bagger.Bag(obj, ActionType.Add)
        val r = bagger.Unbag(record)
        assert(r.intValue == 2)
      })
  }

}
