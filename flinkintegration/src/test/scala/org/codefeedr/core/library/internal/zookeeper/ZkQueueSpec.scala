package org.codefeedr.core.library.internal.zookeeper

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.configuration.{ConfigurationProvider, ConfigurationProviderComponent, ZookeeperConfiguration, ZookeeperConfigurationComponent}
import org.codefeedr.core.LibraryServiceSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

import scala.async.Async.{async, await}
import scala.concurrent.{Await, Future}
import org.codefeedr.util.futureExtensions._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.blocking
import scala.async.Async.await
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Duration, SECONDS}

class ZkQueueSpec extends LibraryServiceSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with LazyLogging

  with ZkStateNodeComponent
  with ZkClientComponent
  with ZookeeperConfigurationComponent
  with ConfigurationProviderComponent
{

  "A ZkQueue" should "expose data in the same order as pushed" in async {
    //Arrange
    val root = new TestRoot()
    await(root.create())

    val d1 = Array[Byte](1.toByte)
    val d2 = Array[Byte](2.toByte)
    val d3 = Array[Byte](3.toByte)

    val l = ListBuffer[Array[Byte]]()
    val queue = new ZkQueue(zkClient, zkClient.prependPath(root.path()))

    val subscription = queue.observe().subscribe(l+=_)

    //Act
    queue.push(d1)
    queue.push(d2)
    queue.push(d3)


    val f = Future {
      blocking {
        while (l.size < 3) {
          Thread.sleep(1)
        }
      }
    }


    await(f)


    //Assert
    assert(f.isCompleted)
    assert( l(0)(0) == d1(0) )
    assert( l(1)(0) == d2(0) )
    assert( l(2)(0) == d3(0) )
  }


  it should "close the observable when the node gets removed" in async {
    //Arrange
    val root = new TestRoot()
    await(root.create())
    val queue = new ZkQueue(zkClient, zkClient.prependPath(root.path()))
    val obs = queue.observe()
    @volatile var end = false
    val f = Future {
      blocking {
        while (!end) {
          Thread.sleep(1)
        }
      }
    }
    val s =obs.subscribe(_ => {}, _ => {}, () => end = true)

    root.delete()
    await(f)

    assert(true)
  }

  it should "fire commands only once" in async {
    //Arrange
    val root = new TestRoot()
    await(root.create())

    val d1 = Array[Byte](1.toByte)
    val d2 = Array[Byte](2.toByte)
    val d3 = Array[Byte](3.toByte)

    val l = ListBuffer[Array[Byte]]()
    val queue = new ZkQueue(zkClient, zkClient.prependPath(root.path()))
    val subscription = queue.observe().subscribe(l += _)


    //Act
    val f1 = Future {
      blocking {
        while (l.size < 1) {
          Thread.sleep(1)
        }
      }
    }
    val f2 = Future {
      blocking {
        while (l.size < 2) {

          Thread.sleep(1)
        }
      }
    }

    //Act
    queue.push(d1)
    await(f1)
    l.clear()
    queue.push(d2)
    queue.push(d3)
    await(f2)



    //Assert
    assert(l.size == 2)
  }

  class TestRoot extends ZkNodeBaseImpl("TestRoot") {
    override def parent(): ZkNodeBase = null

    override def path(): String = s"/$name"
  }


  override lazy val zookeeperConfiguration: ZookeeperConfiguration = libraryServices.zookeeperConfiguration
  override lazy val configurationProvider: ConfigurationProvider = libraryServices.configurationProvider


  /**
    * After each test, make sure to clean the zookeeper store
    */
  override def afterEach(): Unit = {
    super.afterEach()
    Await.ready(zkClient.deleteRecursive("/"), Duration(1, SECONDS))
  }

  /**
    * Also clean up on before each, during development junk might end up in zookeeper when a test crashes
    */
  override def beforeEach(): Unit  = afterEach()
}
