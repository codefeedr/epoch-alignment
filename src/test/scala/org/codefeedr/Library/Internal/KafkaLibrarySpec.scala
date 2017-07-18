package org.codefeedr.Library.Internal

import org.codefeedr.Library.KafkaLibrary
import org.scalatest._

case class TestTypeA(prop1: String)

/**
  * Created by Niels on 18/07/2017.
  */
class KafkaLibrarySpec extends AsyncFlatSpec with Matchers with BeforeAndAfterAll {

  override def beforeAll(): Unit = {

    val f = KafkaLibrary.Initialize()
    while (f.value.isEmpty) {
      Thread.sleep(100)
    }
  }

  override def afterAll(): Unit = {
    val f = KafkaLibrary.Shutdown()
    while (f.value.isEmpty) {
      Thread.sleep(100)
    }
  }

  "A KafkaLibrary" should "be able to register a new type" in {
    for {
      subject <- KafkaLibrary.GetType[TestTypeA]()
    } yield assert(subject.properties.map(o => o.name).contains("prop1"))
  }

  "A KafkaLibrary" should "list all current registered types" in {
    assert(KafkaLibrary.GetSubjectNames().contains("TestTypeA"))
  }

  "A KafkaLibrary" should "be able to remove a registered type again" in {
    for {
      _ <- KafkaLibrary.UnRegisterSubject("TestTypeA")
    } yield assert(!KafkaLibrary.GetSubjectNames().contains("TestTypeA"))
  }

  "A KafkaLibrary" should "return the same subjecttype if GetType is called twice in parallel" in {
    val t1 = KafkaLibrary.GetType[TestTypeA]()
    val t2 = KafkaLibrary.GetType[TestTypeA]()
    for {
      r1 <- t1
      r2 <- t2
    } yield assert(r1.uuid == r2.uuid)
  }

  "A KafkaLibrary" should "return the same subjecttype if GetType is called twice sequential" in {
    for {
      r1 <- KafkaLibrary.GetType[TestTypeA]()
      r2 <- KafkaLibrary.GetType[TestTypeA]()
    } yield assert(r1.uuid == r2.uuid)
  }
}
