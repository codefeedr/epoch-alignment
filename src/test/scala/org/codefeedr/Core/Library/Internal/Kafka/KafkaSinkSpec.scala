

package org.codefeedr.Core.Library.Internal.Kafka

import org.codefeedr.Core.Library.SubjectLibrary
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, BeforeAndAfterEach}

import scala.async.Async.{async, await}
import scala.concurrent.Await
import scala.concurrent.duration.Duration

case class TestKafkaSinkSubject(prop1: String)

class KafkaSinkSpec extends AsyncFlatSpec with BeforeAndAfterEach with BeforeAndAfterAll{

  val testSubjectName = "TestKafkaSinkSubject"

  override def afterEach(): Unit = {
    CleanSubject()
  }

  override def beforeAll(): Unit = {
    Await.ready(SubjectLibrary.Initialized, Duration.Inf)
  }

  def CleanSubject(): Unit =  Await.ready(SubjectLibrary.ForceUnRegisterSubject(testSubjectName), Duration.Inf)


  "A KafkaSink" should "Register and remove itself in the SubjectLibrary" in async {
    val subject = await(SubjectLibrary.GetOrCreateType[TestKafkaSinkSubject]())
    await(SubjectLibrary.RegisterSource(testSubjectName, "SomeSource"))
    val sink = new KafkaGenericSink[TestKafkaSinkSubject](subject)
    assert(!await(SubjectLibrary.GetSinks(testSubjectName)).contains(sink.uuid.toString))
    sink.open(null)
    assert(await(SubjectLibrary.GetSinks(testSubjectName)).contains(sink.uuid.toString))
    sink.close()
    val r = assert(!await(SubjectLibrary.GetSinks(testSubjectName)).contains(sink.uuid.toString))
    await(SubjectLibrary.UnRegisterSource(testSubjectName, "SomeSource"))
    r
  }
}
