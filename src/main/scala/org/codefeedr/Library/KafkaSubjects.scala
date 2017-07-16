package org.codefeedr.Library

import java.util.UUID

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.codefeedr.Library.Internal.{
  KafkaConsumerFactory,
  KafkaProducerFactory,
  SubjectTypeFactory
}
import org.codefeedr.Model.SubjectType

import scala.collection.JavaConverters._
import scala.collection.immutable.Iterable
import scala.concurrent.{CanAwait, ExecutionContext, Future}
import scala.collection.{concurrent, mutable}
import scala.concurrent.duration.Duration
import scala.util.Try

/**
  * Created by Niels on 14/07/2017.
  */
object KafkaSubjects {
  //Topic used to publish all types and topics on
  val SubjectTopic = "Subjects"
  val SubjectAwaitTime = 1000
  val RefreshTime = 1000

  @transient private lazy val SubjectTypeConsumer: KafkaConsumer[String, SubjectType] = {
    //Create consumer that is subscribed to the "subjects" topic
    val consumer = KafkaConsumerFactory.create[String, SubjectType]
    consumer.subscribe(Iterable(SubjectTopic).asJavaCollection)
    consumer
  }

  //Producer to send type information
  @transient private lazy val SubjectTypeProducer: KafkaProducer[String, SubjectType] = {
    KafkaProducerFactory.create[String, SubjectType]
  }

  //Using a concurrent map to keep track of promises that still need to get notified of their type
  @transient private lazy val Subjects: concurrent.Map[String, SubjectType] =
    concurrent.TrieMap[String, SubjectType]()

  //Get a type definition
  def GetType[T](): Future[SubjectType] = {
    val typeDef = SubjectTypeFactory.getSubjectType[T]
    val r = Subjects.get(typeDef.name) map (o => Future { o })
    r.getOrElse(RegisterAndAwaitType[T]())
  }

  private def RegisterAndAwaitType[T](): Future[SubjectType] = Future {
    val typeDef = SubjectTypeFactory.getSubjectType[T]
    SubjectTypeProducer.send(new ProducerRecord(SubjectTopic, typeDef.name, typeDef))
    while (!Subjects.contains(typeDef.name)) {
      Thread.sleep(SubjectAwaitTime)
    }
    Subjects(typeDef.name)
  }

}
