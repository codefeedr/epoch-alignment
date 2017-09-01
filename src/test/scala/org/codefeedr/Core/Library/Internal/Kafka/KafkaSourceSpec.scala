package org.codefeedr.Core.Library.Internal.Kafka

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.codefeedr.Core.Library.SubjectLibrary
import org.codefeedr.Model.TrailedRecord
import org.scalatest.time.Seconds
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, BeforeAndAfterEach}

import scala.async.Async.{async, await}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

case class TestKafkaSourceSubject(prop1: String)

class KafkaSourceSpec extends AsyncFlatSpec with BeforeAndAfterEach with BeforeAndAfterAll{

  val testSubjectName = "TestKafkaSourceSubject"

  override def afterEach(): Unit = {
    CleanSubject()
  }

  override def beforeAll(): Unit = {
    Await.ready(SubjectLibrary.Initialized, Duration(10, SECONDS))
  }

  def CleanSubject(): Unit =  Await.ready(SubjectLibrary.ForceUnRegisterSubject(testSubjectName), Duration(10, SECONDS))


  "A KafkaSource" should "Register and remove itself in the SubjectLibrary" in async {
    val subject = await(SubjectLibrary.GetOrCreateType[TestKafkaSourceSubject](persistent = false))
    val source = new KafkaSource(subject)
    assert(!await(SubjectLibrary.GetSources(testSubjectName)).contains(source.uuid.toString))
    source.InitRun()
    assert(await(SubjectLibrary.GetSources(testSubjectName)).contains(source.uuid.toString))
    assert(source.running)

    val sourceClose = source.AwaitClose()

    //Close the subject so the sink should close itself
    await(SubjectLibrary.Close(testSubjectName))

    //Await the close
    Await.ready(sourceClose, Duration(10, SECONDS))
    assert(!source.running)
    //It should remove itself from the library when it has stopped running, causing the type to be removed
    Await.ready(SubjectLibrary.AwaitClose(testSubjectName), Duration(10, SECONDS))
    assert(true)
  }
}
