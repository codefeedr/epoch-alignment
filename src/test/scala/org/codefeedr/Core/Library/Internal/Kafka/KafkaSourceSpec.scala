package org.codefeedr.Core.Library.Internal.Kafka

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.codefeedr.Core.Library.SubjectLibrary
import org.codefeedr.Model.TrailedRecord
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, BeforeAndAfterEach}

import scala.async.Async.{async, await}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

case class TestKafkaSourceSubject(prop1: String)

class KafkaSourceSpec extends AsyncFlatSpec with BeforeAndAfterEach with BeforeAndAfterAll{

  val testSubjectName = "TestKafkaSourceSubject"

  override def afterEach(): Unit = {
    CleanSubject()
  }

  override def beforeAll(): Unit = {
    Await.ready(SubjectLibrary.Initialized, Duration.Inf)
  }

  def CleanSubject(): Unit =  Await.ready(SubjectLibrary.ForceUnRegisterSubject(testSubjectName), Duration.Inf)


  "A KafkaSource" should "Register and remove itself in the SubjectLibrary" in async {
    val subject = await(SubjectLibrary.GetOrCreateType[TestKafkaSourceSubject]())
    await(SubjectLibrary.RegisterSink(testSubjectName, "SomeSink"))
    val source = new KafkaSource(subject)
    assert(!await(SubjectLibrary.GetSources(testSubjectName)).contains(source.uuid.toString))
    source.InitRun()
    assert(await(SubjectLibrary.GetSources(testSubjectName)).contains(source.uuid.toString))
    assert(source.running)

    //Unregistering should close the subject and stop the sink from running
    await(SubjectLibrary.UnRegisterSink(testSubjectName, "SomeSink"))
    Await.ready(Future {
      while(source.running) {
        Thread.sleep(10)
      }
    }, Duration.fromNanos(1000000L))
    assert(!source.running)
    //It should remove itself from the library when it has stopped running
    source.FinalizeRun()
    assert(!await(SubjectLibrary.GetSources(testSubjectName)).contains(source.uuid.toString))
  }
}
