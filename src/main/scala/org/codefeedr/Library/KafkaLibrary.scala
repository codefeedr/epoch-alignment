package org.codefeedr.Library

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.codefeedr.Library.Internal.{
  KafkaConsumerFactory,
  KafkaController,
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
object KafkaLibrary {
  //Topic used to publish all types and topics on
  //MAke this configurable?
  @transient private val SubjectTopic = "Subjects"
  @transient private val SubjectAwaitTime = 1000
  @transient private val RefreshTime = 1000
  @transient private val PollTimeout = 1000
  @transient private var Initialized = false

  @transient private lazy val subjectTypeConsumer: KafkaConsumer[String, SubjectTypeEvent] = {
    //Create consumer that is subscribed to the "subjects" topic
    val consumer = KafkaConsumerFactory.create[String, SubjectTypeEvent]()
    consumer.subscribe(Iterable(SubjectTopic).asJavaCollection)
    consumer
  }

  //Producer to send type information
  @transient private lazy val subjectTypeProducer: KafkaProducer[String, SubjectTypeEvent] = {
    KafkaProducerFactory.create[String, SubjectTypeEvent]
  }

  @transient private lazy val subjectSynchronizer = new SubjectSynchronizer()

  //Using a concurrent map to keep track of promises that still need to get notified of their type
  //Could switch to a normal map as there should only be a single thread modifying this (but multiple requesting)
  @transient private lazy val subjects: concurrent.Map[String, SubjectType] = {
    concurrent.TrieMap[String, SubjectType]()
  }

  /**
    * Initialize the KafkaLibrary with its subject subscription
    * @return A future when the initialization is done
    */
  def Initialize(): Future[Unit] = {
    KafkaController
      .GuaranteeTopic(SubjectTopic) //This should actually not be needed, as kafka can automatically create topics
      .map(_ => {
        new Thread(subjectSynchronizer).start()
        Initialized = true
      })
  }

  /**
    * Shutdown the kafkalibrary
    * @return
    */
  def Shutdown(): Future[Unit] = {
    assume(Initialized)
    Future {
      subjectSynchronizer.stop()
      while (subjectSynchronizer.running) {
        Thread.sleep(PollTimeout)
      }
      subjectTypeProducer.close()
      subjectTypeConsumer.close()
      Initialized = false
    }
  }

  /**
    * Retrieve a subjectType for an arbitrary scala type
    * Creates type information and registers the type in the library
    * @tparam T The type to register
    * @return The subjectType when it is registered in the library
    */
  def GetType[T: ru.TypeTag](): Future[SubjectType] = {
    assume(Initialized)
    val typeDef = SubjectTypeFactory.getSubjectType[T]
    val r = subjects.get(typeDef.name) map (o => Future { o })
    r.getOrElse(RegisterAndAwaitType[T]())
  }

  /**
    * Retrieves the current set of registered subject names
    * This set might not contain new subjects straight after GetType is called, if the future is not yet completed
    * @return
    */
  def GetSubjectNames(): immutable.Set[String] = {
    assume(Initialized)
    subjects.keys.toSet
  }

  /**
    * Register a type and resolve the future once the type has been registered
    * @tparam T Type to register
    * @return The subjectType once it has been registered
    */
  private def RegisterAndAwaitType[T: ru.TypeTag](): Future[SubjectType] =
    Future {
      val typeDef = SubjectTypeFactory.getSubjectType[T]
      val event = SubjectTypeEvent(typeDef, ActionType.Add)
      subjectTypeProducer.send(new ProducerRecord(SubjectTopic, typeDef.name, event))
      //Not sure if this is the cleanest way to do this
      while (!subjects.contains(typeDef.name)) {
        Thread.sleep(SubjectAwaitTime)
      }
      subjects(typeDef.name)
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
    val event = SubjectTypeEvent(subjects(name), ActionType.Remove)
    subjectTypeProducer.send(new ProducerRecord(SubjectTopic, name, event))
    //Create a future that will wait until the event has been processed
    //Note that this might cause a deadlock when another thread creates the subject again.
    //Should be fixed if this method is made public and used outside unit tests
    Future {
      while (subjects.contains(name)) {
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
      case ActionType.Add    => insert(event.subjectType)
      case ActionType.Update => update(event.subjectType)
      case ActionType.Remove => delete(event.subjectType)
    }

  private def insert(s: SubjectType): Unit =
    if (!subjects.contains(s.name)) {
      subjects.put(s.name, s)
    }

  private def update(s: SubjectType): Unit =
    subjects.put(s.name, s)

  private def delete(s: SubjectType): Unit =
    if (subjects.contains(s.name)) {
      subjects.remove(s.name)
    }

  class SubjectSynchronizer extends Runnable {
    var running = false

    def run(): Unit = {
      running = true
      while (running) {
        subjectTypeConsumer
          .poll(PollTimeout)
          .iterator()
          .asScala
          .map(o => o.value())
          .foreach(handleEvent)
        Thread.sleep(RefreshTime)
      }
    }

    def stop(): Unit = {
      running = false
    }
  }
}
