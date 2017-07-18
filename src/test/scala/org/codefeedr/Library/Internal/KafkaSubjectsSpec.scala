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
}
