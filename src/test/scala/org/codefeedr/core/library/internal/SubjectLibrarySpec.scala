

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

import org.codefeedr.core.Library.Internal.Zookeeper.ZkClient
import org.codefeedr.core.{LibraryServiceSpec, ZkTest}
import org.codefeedr.core.Library.LibraryServices
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
/*
class subjectLibrarySpec extends LibraryServiceSpec with BeforeAndAfterAll with BeforeAndAfterEach {

  implicit override def executionContext: ExecutionContextExecutor =
    scala.concurrent.ExecutionContext.Implicits.global

  val TestTypeName = "TestTypeA"
  val SinkUuid = "ThisIsSinkUUID"
  val SourceUuid = "ThisIsSourceUUID"

  override def beforeAll(): Unit = {

  }

  override def beforeEach(): Unit = {
    Await.ready(zkClient.DeleteRecursive("/"), Duration(1, SECONDS))
    Await.ready(subjectLibrary.Initialize(), Duration(1, SECONDS))
  }

  def assertFails[TException<: Exception: ClassTag](f:Future[_]): Future[Assertion] = async {
    val exception = await(f.failed)
    assert(classTag[TException].runtimeClass.isInstance(exception))
  }


  "subjectLibrary" should "be able to register and remove a new type" taggedAs (Slow, ZkTest) in async {
      val children = await(subjectLibrary.GetSubjectNames())
      assert(!children.contains(TestTypeName))
      val subject = await(subjectLibrary.GetOrCreateType[TestTypeA]())
      assert(subject.properties.map(o => o.name).contains("prop1"))
      assert(await(subjectLibrary.GetSubjectNames()).contains(TestTypeName))
      assert(await(subjectLibrary.IsOpen(TestTypeName)))
      assert(await(subjectLibrary.UnRegisterSubject(TestTypeName)))
      assert(!await(subjectLibrary.GetSubjectNames()).contains(TestTypeName))
    }

    it should "construct subjects as open by default" in async {
      await(subjectLibrary.GetOrCreateType[TestTypeA]())
      assert(await(subjectLibrary.IsOpen(TestTypeName)))
    }

      it should "be possible to close subject types" in async {
        await(subjectLibrary.GetOrCreateType[TestTypeA]())
        assert(await(subjectLibrary.IsOpen(TestTypeName)))
        await(subjectLibrary.Close(TestTypeName))
        assert(!await(subjectLibrary.IsOpen(TestTypeName)))
      }

      "subjectLibrary.AwaitClose" should "Return a future that resolves when OnClose is called" in async {
        await(subjectLibrary.GetOrCreateType[TestTypeA]())
        val f = subjectLibrary.AwaitClose(TestTypeName)
        assertThrows[TimeoutException](Await.ready(f, Duration(100, MILLISECONDS)))
        subjectLibrary.Close(TestTypeName)
        await(f)
        assert(!await(subjectLibrary.IsOpen(TestTypeName)))
      }


      "subjectLibrary.Delete" should "return false if called twice" in async {
        await(subjectLibrary.GetOrCreateType[TestTypeA]())
        assert(await(subjectLibrary.UnRegisterSubject(TestTypeName)))
        assert(!await(subjectLibrary.UnRegisterSubject(TestTypeName)))
      }

      "subjectLibrary.Delete" should "return false if called on a non existing type" taggedAs(Slow, ZkTest) in async {
        assert(!await(subjectLibrary.UnRegisterSubject("SomeNonExistingType")))
      }


  //TODO: Support this? Not very relevant because types are currently created by a single manager
  /*
  "subjectLibrary.GetType" should "return the same subjecttype if GetType is called twice in parallel" taggedAs (Slow, ZkTest) in {
    val t1 = subjectLibrary.GetOrCreateType[TestTypeA]()
    val t2 = subjectLibrary.GetOrCreateType[TestTypeA]()
    for {
      r1 <- t1
      r2 <- t2
    } yield assert(r1.uuid == r2.uuid)
  }
*/


  "subjectLibrary.GetType" should "return the same subjecttype if called twice sequential" taggedAs (Slow, ZkTest) in async {
    val r1 = await(subjectLibrary.GetOrCreateType[TestTypeA]())
    val r2 = await(subjectLibrary.GetOrCreateType[TestTypeA]())
    val result = assert(r1.uuid == r2.uuid)
    result
  }


  "subjectLibrary.AwaitTypeRegistration" should "Resolve the future when the type is registered" in async {
    val resolve = subjectLibrary.AwaitTypeRegistration(TestTypeName)
    assertThrows[TimeoutException](Await.ready(resolve, Duration(100, MILLISECONDS)))
    val r = await(subjectLibrary.GetOrCreateType[TestTypeA]())
    val resolved = await(resolve)
    assert(resolved.uuid == r.uuid)
  }

  "subjectLibrary.GetSinks" should "Be able to retrieve registered sinks" in async {
    await(subjectLibrary.GetOrCreateType[TestTypeA]())
    assert(await(subjectLibrary.GetSinks(TestTypeName)).isEmpty)
    await(subjectLibrary.RegisterSink(TestTypeName,SinkUuid))
    assert(await(subjectLibrary.GetSinks(TestTypeName)).contains(SinkUuid))
  }
  "subjectLibrary.GetSources" should "Be able to retrieve registered sources" in async {
    await(subjectLibrary.GetOrCreateType[TestTypeA]())
    assert(await(subjectLibrary.GetSources(TestTypeName)).isEmpty)
    await(subjectLibrary.RegisterSource(TestTypeName,SourceUuid))
    assert(await(subjectLibrary.GetSources(TestTypeName)).contains(SourceUuid))
  }

  "subjectLibrary.RegisterSink" should "Throw an exception when called twice" in async {
    await(subjectLibrary.GetOrCreateType[TestTypeA]())
    await(subjectLibrary.RegisterSink(TestTypeName,SinkUuid))
    await(assertFails[SinkAlreadySubscribedException](subjectLibrary.RegisterSink(TestTypeName,SinkUuid)))
  }
  "subjectLibrary.RegisterSource" should "Throw an exception when called twice" in async {
    await(subjectLibrary.GetOrCreateType[TestTypeA]())
    await(subjectLibrary.RegisterSource(TestTypeName,SourceUuid))
    await(assertFails[SourceAlreadySubscribedException](subjectLibrary.RegisterSource(TestTypeName,SourceUuid)))
  }

  "subjectLibrary.RegisterSink" should "Throw an exception the type name does not exist" in {
    assertFails[TypeNameNotFoundException](subjectLibrary.RegisterSink(TestTypeName,SinkUuid))
  }
  "subjectLibrary.RegisterSource" should "Throw an exception the type name does not exist" in {
    assertFails[TypeNameNotFoundException](subjectLibrary.RegisterSource(TestTypeName,SourceUuid))
  }



  "subjectLibrary.UnRegisterSink" should "Throw an exception when sink is not registered" in async {
    await(subjectLibrary.GetOrCreateType[TestTypeA]())
    await(assertFails[SinkNotSubscribedException](subjectLibrary.UnRegisterSink(TestTypeName,SinkUuid)))
  }

  "subjectLibrary.UnRegisterSource" should "Throw an exception when sink is not registered" in async {
    await(subjectLibrary.GetOrCreateType[TestTypeA]())
    await(assertFails[SourceNotSubscribedException](subjectLibrary.UnRegisterSource(TestTypeName,SourceUuid)))
  }


  "subjectLibrary.HasSink" should "Return true when a type has a sink and false otherwise" in async {
    await(subjectLibrary.GetOrCreateType[TestTypeA](persistent = true))
    assert(!await(subjectLibrary.HasSinks(TestTypeName)))
    await(subjectLibrary.RegisterSink(TestTypeName,SinkUuid))
    assert(await(subjectLibrary.HasSinks(TestTypeName)))
    await(subjectLibrary.UnRegisterSink(TestTypeName,SinkUuid))
    assert(!await(subjectLibrary.HasSinks(TestTypeName)))
  }

  "subjectLibrary.HasSource" should "Return true when a type has a source and false otherwise" in async {
    await(subjectLibrary.GetOrCreateType[TestTypeA](persistent = true))
    assert(!await(subjectLibrary.HasSources(TestTypeName)))
    await(subjectLibrary.RegisterSource(TestTypeName,SourceUuid))
    assert(await(subjectLibrary.HasSources(TestTypeName)))
    await(subjectLibrary.UnRegisterSource(TestTypeName,SourceUuid))
    assert(!await(subjectLibrary.HasSources(TestTypeName)))
  }

  "subjectLibrary.UnregisterSubject" should "Throw an exception when the given subject has an active sink" in async {
    await(subjectLibrary.GetOrCreateType[TestTypeA]())
    await(subjectLibrary.RegisterSink(TestTypeName, SinkUuid))
    await(assertFails[ActiveSinkException](subjectLibrary.UnRegisterSubject(TestTypeName)))
  }

  "subjectLibrary.UnregisterSource" should "Throw an exception when the given subject has an active source" in async {
    await(subjectLibrary.GetOrCreateType[TestTypeA]())
    await(subjectLibrary.RegisterSource(TestTypeName, SourceUuid))
    await(assertFails[ActiveSourceException](subjectLibrary.UnRegisterSubject(TestTypeName)))
  }

  "subjectLibrary.UnregisterSink" should "close the subject is not persistent and all sinks are removed" in async {
    await(subjectLibrary.GetOrCreateType[TestTypeA]())
    await(subjectLibrary.RegisterSink(TestTypeName, "Sink1"))
    await(subjectLibrary.RegisterSink(TestTypeName, "Sink2"))
    //Register a source because otherwise the type would be removed
    await(subjectLibrary.RegisterSource(TestTypeName, SourceUuid))
    assert(await(subjectLibrary.IsOpen(TestTypeName)))
    await(subjectLibrary.UnRegisterSink(TestTypeName,"Sink1"))
    assert(await(subjectLibrary.IsOpen(TestTypeName)))
    await(subjectLibrary.UnRegisterSink(TestTypeName,"Sink2"))
    assert(!await(subjectLibrary.IsOpen(TestTypeName)))
  }
/*
  TODO: This functionality is disabled because we need to discuss if we want this functionality. Auto-removing subjects might confuse users and makes writing tests harder
  "subjectLibrary.UnregisterSink" should "remove the type if a non-persistent subject no longer has any sinks/sources" in async {
    await(subjectLibrary.GetOrCreateType[TestTypeA]())
    await(subjectLibrary.RegisterSink(TestTypeName, SinkUuid))
    assert(await(subjectLibrary.Exists(TestTypeName)))
    await(subjectLibrary.UnRegisterSink(TestTypeName, SinkUuid))
    assert(!await(subjectLibrary.Exists(TestTypeName)))
  }

  "subjectLibrary.UnregisterSource" should "remove the type if a non-persistent subject no longer has any sinks/sources" in async {
    await(subjectLibrary.GetOrCreateType[TestTypeA]())
    await(subjectLibrary.RegisterSource(TestTypeName, SourceUuid))
    assert(await(subjectLibrary.Exists(TestTypeName)))
    await(subjectLibrary.UnRegisterSource(TestTypeName, SourceUuid))
    assert(!await(subjectLibrary.Exists(TestTypeName)))
  }
*/
}
*/