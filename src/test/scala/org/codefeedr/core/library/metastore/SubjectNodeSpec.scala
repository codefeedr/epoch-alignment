package org.codefeedr.core.library.metastore

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.core.LibraryServiceSpec
import org.codefeedr.core.library.MyOwnIntegerObject
import org.codefeedr.core.library.internal.SubjectTypeFactory
import org.codefeedr.core.library.internal.kafka.TestKafkaSourceSubject
import org.codefeedr.model.SubjectType
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

import scala.async.Async.{async, await}
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}




class SubjectNodeSpec  extends LibraryServiceSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with LazyLogging{


  "GetOrCreate" should "create a node in the open state" in async {
    val subjectNode = subjectLibrary.getSubject[MyOwnIntegerObject]()
    await(subjectNode.getOrCreateType[MyOwnIntegerObject]())
    assert(await(subjectNode.getState()).get)
  }



  /**
    * After each test, make sure to clean the zookeeper store
    */
  override def afterEach(): Unit = {
    Await.ready(zkClient.deleteRecursive("/"), Duration(1, SECONDS))
  }
}
