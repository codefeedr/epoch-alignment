

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

package org.codefeedr.Core.Library.Internal

import org.codefeedr.Core.ZkTest
import org.codefeedr.Core.Library.SubjectLibrary
import org.codefeedr.Exceptions._
import org.scalatest._
import org.scalatest.tagobjects.Slow
import org.scalatest.time.Milliseconds

import scala.async.Async.{async, await}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future, TimeoutException}
import scala.reflect.{ClassTag, classTag}

case class TestTypeA(prop1: String)

/**
  * Created by Niels on 18/07/2017.
  */
class SubjectLibrarySpec extends AsyncFlatSpec with BeforeAndAfterAll with BeforeAndAfterEach{
  implicit override def executionContext: ExecutionContextExecutor =
    scala.concurrent.ExecutionContext.Implicits.global

  val TestTypeName = "TestTypeA"
  val SinkUuid = "ThisIsSinkUUID"
  val SourceUuid = "ThisIsSourceUUID"

  override def beforeAll(): Unit = {
    Await.ready(SubjectLibrary.Initialized, Duration.Inf)
  }

  def CleanSubject(): Unit =  Await.ready(SubjectLibrary.ForceUnRegisterSubject(TestTypeName), Duration.Inf)

  def assertFails[TException<: Exception: ClassTag](f:Future[_]): Future[Assertion] = async {
    val exception = await(f.failed)
    assert(classTag[TException].runtimeClass.isInstance(exception))
  }

  override def afterEach(): Unit = {
    CleanSubject()
  }

  behavior of "SubjectLibrary"

  it should "be able to register and remove a new type" taggedAs (Slow, ZkTest) in async {
    assert(!await(SubjectLibrary.GetSubjectNames()).contains(TestTypeName))
    val subject = await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    assert(subject.properties.map(o => o.name).contains("prop1"))
    assert(await(SubjectLibrary.GetSubjectNames()).contains(TestTypeName))
    assert(await(SubjectLibrary.IsOpen(TestTypeName)))
    assert(await(SubjectLibrary.UnRegisterSubject(TestTypeName)))
    assert(!await(SubjectLibrary.GetSubjectNames()).contains(TestTypeName))
  }

  it should "construct subjects as open by default" in async {
    await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    assert(await(SubjectLibrary.IsOpen(TestTypeName)))
  }

  it should "be possible to close subject types" in async {
    await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    assert(await(SubjectLibrary.IsOpen(TestTypeName)))
    await(SubjectLibrary.Close(TestTypeName))
    assert(!await(SubjectLibrary.IsOpen(TestTypeName)))
  }

  "SubjectLibrary.AwaitClose" should "Return a future that resolves when OnClose is called" in async {
    await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    val f = SubjectLibrary.AwaitClose(TestTypeName)
    assertThrows[TimeoutException](Await.ready(f, Duration(100, MILLISECONDS)))
    SubjectLibrary.Close(TestTypeName)
    await(f)
    assert(!await(SubjectLibrary.IsOpen(TestTypeName)))
  }


  "SubjectLibrary.Delete" should "return false if called twice" in async {
    await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    assert(await(SubjectLibrary.UnRegisterSubject(TestTypeName)))
    assert(!await(SubjectLibrary.UnRegisterSubject(TestTypeName)))
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
    val resolve = SubjectLibrary.AwaitTypeRegistration(TestTypeName)
    assert(!resolve.isCompleted)
    val r = await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    assert(await(resolve).uuid == r.uuid)
  }

  "SubjectLibrary.GetSinks" should "Be able to retrieve registered sinks" in async {
    await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    assert(await(SubjectLibrary.GetSinks(TestTypeName)).isEmpty)
    await(SubjectLibrary.RegisterSink(TestTypeName,SinkUuid))
    assert(await(SubjectLibrary.GetSinks(TestTypeName)).contains(SinkUuid))
  }
  "SubjectLibrary.GetSources" should "Be able to retrieve registered sources" in async {
    await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    assert(await(SubjectLibrary.GetSources(TestTypeName)).isEmpty)
    await(SubjectLibrary.RegisterSource(TestTypeName,SourceUuid))
    assert(await(SubjectLibrary.GetSources(TestTypeName)).contains(SourceUuid))
  }

  "SubjectLibrary.RegisterSink" should "Throw an exception when called twice" in async {
    await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    await(SubjectLibrary.RegisterSink(TestTypeName,SinkUuid))
    await(assertFails[SinkAlreadySubscribedException](SubjectLibrary.RegisterSink(TestTypeName,SinkUuid)))
  }
  "SubjectLibrary.RegisterSource" should "Throw an exception when called twice" in async {
    await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    await(SubjectLibrary.RegisterSource(TestTypeName,SourceUuid))
    await(assertFails[SourceAlreadySubscribedException](SubjectLibrary.RegisterSource(TestTypeName,SourceUuid)))
  }

  "SubjectLibrary.RegisterSink" should "Throw an exception the type name does not exist" in {
    assertFails[TypeNameNotFoundException](SubjectLibrary.RegisterSink(TestTypeName,SinkUuid))
  }
  "SubjectLibrary.RegisterSource" should "Throw an exception the type name does not exist" in {
    assertFails[TypeNameNotFoundException](SubjectLibrary.RegisterSource(TestTypeName,SourceUuid))
  }



  "SubjectLibrary.UnRegisterSink" should "Throw an exception when sink is not registered" in async {
    await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    await(assertFails[SinkNotSubscribedException](SubjectLibrary.UnRegisterSink(TestTypeName,SinkUuid)))
  }

  "SubjectLibrary.UnRegisterSource" should "Throw an exception when sink is not registered" in async {
    await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    await(assertFails[SourceNotSubscribedException](SubjectLibrary.UnRegisterSource(TestTypeName,SourceUuid)))
  }


  "SubjectLibrary.HasSink" should "Return true when a type has a sink and false otherwise" in async {
    await(SubjectLibrary.GetOrCreateType[TestTypeA](persistent = true))
    assert(!await(SubjectLibrary.HasSinks(TestTypeName)))
    await(SubjectLibrary.RegisterSink(TestTypeName,SinkUuid))
    assert(await(SubjectLibrary.HasSinks(TestTypeName)))
    await(SubjectLibrary.UnRegisterSink(TestTypeName,SinkUuid))
    assert(!await(SubjectLibrary.HasSinks(TestTypeName)))
  }

  "SubjectLibrary.HasSource" should "Return true when a type has a source and false otherwise" in async {
    await(SubjectLibrary.GetOrCreateType[TestTypeA](persistent = true))
    assert(!await(SubjectLibrary.HasSources(TestTypeName)))
    await(SubjectLibrary.RegisterSource(TestTypeName,SourceUuid))
    assert(await(SubjectLibrary.HasSources(TestTypeName)))
    await(SubjectLibrary.UnRegisterSource(TestTypeName,SourceUuid))
    assert(!await(SubjectLibrary.HasSources(TestTypeName)))
  }

  "SubjectLibrary.UnregisterSubject" should "Throw an exception when the given subject has an active sink" in async {
    await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    await(SubjectLibrary.RegisterSink(TestTypeName, SinkUuid))
    await(assertFails[ActiveSinkException](SubjectLibrary.UnRegisterSubject(TestTypeName)))
  }

  "SubjectLibrary.UnregisterSource" should "Throw an exception when the given subject has an active source" in async {
    await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    await(SubjectLibrary.RegisterSource(TestTypeName, SourceUuid))
    await(assertFails[ActiveSourceException](SubjectLibrary.UnRegisterSubject(TestTypeName)))
  }

  "SubjectLibrary.UnregisterSink" should "close the subject is not persistent and all sinks are removed" in async {
    await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    await(SubjectLibrary.RegisterSink(TestTypeName, "Sink1"))
    await(SubjectLibrary.RegisterSink(TestTypeName, "Sink2"))
    //Register a source because otherwise the type would be removed
    await(SubjectLibrary.RegisterSource(TestTypeName, SourceUuid))
    assert(await(SubjectLibrary.IsOpen(TestTypeName)))
    await(SubjectLibrary.UnRegisterSink(TestTypeName,"Sink1"))
    assert(await(SubjectLibrary.IsOpen(TestTypeName)))
    await(SubjectLibrary.UnRegisterSink(TestTypeName,"Sink2"))
    assert(!await(SubjectLibrary.IsOpen(TestTypeName)))
  }

  "SubjectLibrary.UnregisterSink" should "remove the type if a non-persistent subject no longer has any sinks/sources" in async {
    await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    await(SubjectLibrary.RegisterSink(TestTypeName, SinkUuid))
    assert(await(SubjectLibrary.Exists(TestTypeName)))
    await(SubjectLibrary.UnRegisterSink(TestTypeName, SinkUuid))
    assert(!await(SubjectLibrary.Exists(TestTypeName)))
  }

  "SubjectLibrary.UnregisterSource" should "remove the type if a non-persistent subject no longer has any sinks/sources" in async {
    await(SubjectLibrary.GetOrCreateType[TestTypeA]())
    await(SubjectLibrary.RegisterSource(TestTypeName, SourceUuid))
    assert(await(SubjectLibrary.Exists(TestTypeName)))
    await(SubjectLibrary.UnRegisterSource(TestTypeName, SourceUuid))
    assert(!await(SubjectLibrary.Exists(TestTypeName)))
  }

}
