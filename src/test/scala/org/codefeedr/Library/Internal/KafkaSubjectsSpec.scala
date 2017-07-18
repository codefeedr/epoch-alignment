package org.codefeedr.Library.Internal

import org.codefeedr.Library.KafkaSubjects
import org.scalatest.{AsyncFlatSpec, Matchers}

case class TestTypeA(prop1: String)

/**
  * Created by Niels on 18/07/2017.
  */
class KafkaSubjectsSpec extends AsyncFlatSpec with Matchers {
  "A KafkaSubjects" should "be able to register a new type" in {
    for {
      subject <- KafkaSubjects.GetType[TestTypeA]()
    } yield assert(subject.properties.map(o => o.name).contains("prop1"))
  }

  "A KafkaSubjects" should "list all current registered types" in {
    assert(KafkaSubjects.GetSubjectNames().contains("TestTypeA"))
  }

  "A KafkaSubjects" should "be able to remove a registered type again" in {
    for {
      _ <- KafkaSubjects.UnRegisterSubject("TestTypeA")
    } yield assert(!KafkaSubjects.GetSubjectNames().contains("TestTypeA"))
  }

  "A KafkaSubjects" should "return the same subjecttype if GetType is called twice in parallel" in {
    val t1 = KafkaSubjects.GetType[TestTypeA]()
    val t2 = KafkaSubjects.GetType[TestTypeA]()
    for {
      r1 <- t1
      r2 <- t2
    } yield assert(r1.uuid == r2.uuid)
  }

  "A KafkaSubjects" should "return the same subjecttype if GetType is called twice sequential" in {
    for {
      r1 <- KafkaSubjects.GetType[TestTypeA]()
      r2 <- KafkaSubjects.GetType[TestTypeA]()
    } yield assert(r1.uuid == r2.uuid)
  }
}
