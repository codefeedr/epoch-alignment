

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

package org.codefeedr.core.library

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.core.library.internal.{RecordTransformer, RecordUtils, SubjectTypeFactory}
import org.codefeedr.Model.ActionType
import org.scalatest.{AsyncFlatSpec, Matchers}

case class BagTestInt(intValue: Int)
case class BagTestString(stringValue: String)
case class BagTestObject(objectValue: Object)
class BagTestPojo(var intValue: Int)

/**
  * Created by Niels on 28/07/2017.
  */
class RecordTransformerSpec extends AsyncFlatSpec with Matchers with LazyLogging {

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
