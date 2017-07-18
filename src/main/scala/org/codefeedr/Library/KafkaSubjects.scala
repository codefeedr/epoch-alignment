package org.codefeedr.Library

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.codefeedr.Library.Internal.{
  KafkaConsumerFactory,
  KafkaProducerFactory,
  SubjectTypeFactory
}
import org.codefeedr.Model.{ActionType, SubjectType, SubjectTypeEvent}

import scala.collection.JavaConverters._
import scala.collection.immutable.Iterable
import scala.collection.{concurrent, immutable}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.runtime.{universe => ru}

/**
  * Created by Niels on 14/07/2017.
  */
object KafkaSubjects {
  //Topic used to publish all types and topics on
  val SubjectTopic = "Subjects"
  val SubjectAwaitTime = 1000
  val RefreshTime = 1000
  val PollTimeout = 1000

  @transient private lazy val subjectTypeConsumer: KafkaConsumer[String, SubjectTypeEvent] = {
    //Create consumer that is subscribed to the "subjects" topic
    val consumer = KafkaConsumerFactory.create[String, SubjectTypeEvent]()
    consumer.subscribe(Iterable(SubjectTopic).asJavaCollection)
    consumer
  }

  //Producer to send type information
  @transient private lazy val SubjectTypeProducer: KafkaProducer[String, SubjectTypeEvent] = {
    KafkaProducerFactory.create[String, SubjectTypeEvent]
  }

  //Using a concurrent map to keep track of promises that still need to get notified of their type
  @transient private lazy val Subjects: concurrent.Map[String, SubjectType] = {
    new Thread {
      override def run(): Unit =
        while (true) {
          Thread.sleep(RefreshTime)
          subjectTypeConsumer
            .poll(PollTimeout)
            .iterator()
            .asScala
            .map(o => o.value())
            .foreach(handleEvent)
        }
    }.start()
    concurrent.TrieMap[String, SubjectType]()
  }

  /**
    * Retrieve a subjectType for an arbitrary scala type
    * Creates type information and registers the type in the library
    * @tparam T The type to register
    * @return The subjectType when it is registered in the library
    */
  def GetType[T: ru.TypeTag](): Future[SubjectType] = {
    val typeDef = SubjectTypeFactory.getSubjectType[T]
    val r = Subjects.get(typeDef.name) map (o => Future { o })
    r.getOrElse(RegisterAndAwaitType[T]())
  }

  /**
    * Retrieves the current set of registered subject names
    * This set might not contain new subjects straight after GetType is called, if the future is not yet completed
    * @return
    */
  def GetSubjectNames(): immutable.Set[String] = Subjects.keys.toSet

  /**
    * Register a type and resolve the future once the type has been registered
    * @tparam T Type to register
    * @return The subjectType once it has been registered
    */
  private def RegisterAndAwaitType[T: ru.TypeTag](): Future[SubjectType] = Future {
    val typeDef = SubjectTypeFactory.getSubjectType[T]
    val event = SubjectTypeEvent(typeDef, ActionType.Add)
    SubjectTypeProducer.send(new ProducerRecord(SubjectTopic, typeDef.name, event))
    //Not sure if this is the cleanest way to do this
    while (!Subjects.contains(typeDef.name)) {
      Thread.sleep(SubjectAwaitTime)
    }
    Subjects(typeDef.name)
  }

  /**
    * Un-register a subject from the library
    * This method is mainly added to make the unit tests have no side-effect, but should likely not be exposed or used in the final product
    * This method currently has unwanted side-effects, and should not be made public in its current state
    * @param name: String
    * @return A future that returns when the subject has actually been removed from the library
    */
  private[Library] def UnRegisterSubject(name: String): Future[Unit] = {
    //Send the removal event
    //Note that this causes an exception if the type is actually not registered
    val event = SubjectTypeEvent(Subjects(name), ActionType.Remove)
    SubjectTypeProducer.send(new ProducerRecord(SubjectTopic, name, event))
    //Create a future that will wait until the event has been processed
    //Note that this might cause a deadlock when another thread creates the subject again.
    //Should be fixed if this method is made public and used outside unit tests
    Future {
      while (Subjects.contains(name)) {
        Thread.sleep(SubjectAwaitTime)
      }
    }
  }

  /**
    * Event handler managing the internal state of registered subjects
    * @param event: SubjectTypeEvent
    */
  private def handleEvent(event: SubjectTypeEvent): Unit =
    event.actionType match {
      case ActionType.Add => insert(event.subjectType)
      case ActionType.Update => update(event.subjectType)
      case ActionType.Remove => delete(event.subjectType)
    }

  private def insert(s: SubjectType): Unit =
    if (!Subjects.contains(s.name)) {
      Subjects.put(s.name, s)
    }

  private def update(s: SubjectType): Unit =
    Subjects.put(s.name, s)

  private def delete(s: SubjectType): Unit =
    if (Subjects.contains(s.name)) {
      Subjects.remove(s.name)
    }

}
