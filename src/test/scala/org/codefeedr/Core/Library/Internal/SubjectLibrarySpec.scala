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

package org.codefeedr.Core.Library.Internal

import org.codefeedr.Core.ZkTest
import org.codefeedr.Core.Library.SubjectLibrary
import org.scalactic.source.Position
import org.scalatest._
import org.scalatest.tagobjects.Slow

import scala.async.Async.{async, await}
import scala.concurrent.{ExecutionContextExecutor, Future}

case class TestTypeA(prop1: String)

/**
  * Created by Niels on 18/07/2017.
  */
class SubjectLibrarySpec extends AsyncFlatSpec with BeforeAndAfterAll with BeforeAndAfterEach{
  implicit override def executionContext: ExecutionContextExecutor =
    scala.concurrent.ExecutionContext.Implicits.global

  override def beforeAll(): Unit = {
    //TODO: If someone knows a better way to await a future in beforeAll, please let me know
    while(!SubjectLibrary.Initialized.isCompleted) {
      Thread.sleep(10)
    }
  }

  override def afterEach(): Unit = {
    val r = SubjectLibrary.UnRegisterSubject("TestTypeA")
    while(!r.isCompleted) {
      Thread.sleep(10)
    }
  }

  behavior of "SubjectLibrary"

  it should "be able to register and remove a new type" taggedAs (Slow, ZkTest) in async {
    assert(!await(SubjectLibrary.GetSubjectNames()).contains("TestTypeA"))
    val subject = await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    assert(subject.properties.map(o => o.name).contains("prop1"))
    assert(await(SubjectLibrary.GetSubjectNames()).contains("TestTypeA"))
    assert(await(SubjectLibrary.UnRegisterSubject("TestTypeA")))
    assert(!await(SubjectLibrary.GetSubjectNames()).contains("TestTypeA"))
  }

  it should "construct subjects as open by default" in async {
    await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    assert(await(SubjectLibrary.IsOpen("TestTypeA")))
  }

  it should "be possible to close subject types" in async {
    await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    assert(await(SubjectLibrary.IsOpen("TestTypeA")))
    await(SubjectLibrary.Close("TestTypeA"))
    assert(!await(SubjectLibrary.IsOpen("TestTypeA")))
  }

  "SubjectLibrary.AwaitClose" should "Return a future that resolves when OnClose is called" in async {
    await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    val f = SubjectLibrary.awaitClose("TestTypeA")
    assert(!f.isCompleted)
    await(SubjectLibrary.Close("TestTypeA"))
    await(f)
    assert(!await(SubjectLibrary.IsOpen("TestTypeA")))
  }


  "SubjectLibrary.Delete" should "return false if called twice" in async {
    await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    assert(await(SubjectLibrary.UnRegisterSubject("TestTypeA")))
    assert(!await(SubjectLibrary.UnRegisterSubject("TestTypeA")))
  }

  "SubjectLibrary.Delete" should "return false if called on a non existing type" taggedAs(Slow, ZkTest) in async {
    assert(!await(SubjectLibrary.UnRegisterSubject("SomeNonExistingType")))
  }

  "SubjectLibrary.GetType" should "return the same subjecttype if GetType is called twice in parallel" taggedAs (Slow, ZkTest) in {
    val t1 = SubjectLibrary.GetOrCreateType[TestTypeA]()
    val t2 = SubjectLibrary.GetOrCreateType[TestTypeA]()
    for {
      r1 <- t1
      r2 <- t2
    } yield assert(r1.uuid == r2.uuid)
  }

  "SubjectLibrary.GetType" should "return the same subjecttype if called twice sequential" taggedAs (Slow, ZkTest) in async {
    val r1 = await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    val r2 = await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    val result = assert(r1.uuid == r2.uuid)
    result
  }

  "SubjectLibrary.AwaitTypeRegistration" should "Resolve the future when the type is registered" in async {
    val resolve = SubjectLibrary.AwaitTypeRegistration("TestTypeA")
    assert(!resolve.isCompleted)
    val r = await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    assert(await(resolve).uuid == r.uuid)
  }



}
