/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.codefeedr.Library.Internal

import java.util.UUID

import org.apache.commons.lang3.Conversion.uuidToByteArray
import org.codefeedr.Model.ActionType
import org.scalatest.{FlatSpec, Matchers}



case class KeyFactoryTestClass(a: String, b: String, c: Int)

/**
  * Created by Niels on 31/07/2017.
  */
class KeyFactorySpec extends FlatSpec with Matchers {
  "A KeyFactory" should "Generate new unique keys if no keys are defined in " in {
    val t = SubjectTypeFactory.getSubjectType[KeyFactoryTestClass]
    val factory = new KeyFactory(t, UUID.randomUUID())
    val transformer = new RecordTransformer[KeyFactoryTestClass](t)
    val record1 = transformer.Bag(KeyFactoryTestClass("a", "b", 12), ActionType.Add)
    val record2 = transformer.Bag(KeyFactoryTestClass("a", "b", 12), ActionType.Add)
    val k1 = factory.GetKey(record1)
    val k2 = factory.GetKey(record2)
    assert(!(k1.Key sameElements k2.Key))
  }

  "A KeyFactory" should "assign the given uuid" in {
    val t = SubjectTypeFactory.getSubjectType[KeyFactoryTestClass]
    val uuid = UUID.randomUUID()
    val factory = new KeyFactory(t, uuid)
    val transformer = new RecordTransformer[KeyFactoryTestClass](t)
    val record1 = transformer.Bag(KeyFactoryTestClass("a", "b", 12), ActionType.Add)
    val k1 = factory.GetKey(record1)
    assert(k1.SourceId sameElements uuidToByteArray(uuid, new Array[Byte](16), 0, 16))
  }

  "A KeyFactory" should "Use keyfields to generate a key if defined" in {
    val t = SubjectTypeFactory.getSubjectType[KeyFactoryTestClass](Array("a"))
    val factory = new KeyFactory(t, UUID.randomUUID())
    val transformer = new RecordTransformer[KeyFactoryTestClass](t)
    val record1 = transformer.Bag(KeyFactoryTestClass("a", "b", 12), ActionType.Add)
    val record2 = transformer.Bag(KeyFactoryTestClass("a", "b", 12), ActionType.Add)
    val k1 = factory.GetKey(record1)
    val k2 = factory.GetKey(record2)
    assert(k1.Key sameElements k2.Key)
  }

  "A KeyFactory" should "Use all keyfields to generate a key if defined" in {
    val t = SubjectTypeFactory.getSubjectType[KeyFactoryTestClass](Array("a", "b"))
    val factory = new KeyFactory(t, UUID.randomUUID())
    val transformer = new RecordTransformer[KeyFactoryTestClass](t)
    val record1 = transformer.Bag(KeyFactoryTestClass("a", "b", 12), ActionType.Add)
    val record2 = transformer.Bag(KeyFactoryTestClass("a", "b", 12), ActionType.Add)
    val record3 = transformer.Bag(KeyFactoryTestClass("a", "c", 12), ActionType.Add)
    val k1 = factory.GetKey(record1)
    val k2 = factory.GetKey(record2)
    val k3 = factory.GetKey(record3)
    assert(k1.Key sameElements k2.Key)
    assert(!(k2.Key sameElements k3.Key))
  }
}
