package org.codefeedr.core.library.internal.zookeeper

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.configuration.{ConfigurationProvider, ConfigurationProviderComponent, ZookeeperConfiguration, ZookeeperConfigurationComponent}
import org.codefeedr.core.LibraryServiceSpec
import org.codefeedr.util.futureExtensions._
import org.codefeedr.util.observableExtension._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

import scala.async.Async.{async, await}
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}
import scala.concurrent.{Await, Future}


/**
  * Testclass for  [[ZkNode]]
  */
class ZkNodeSpec extends LibraryServiceSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with LazyLogging
  with ZkStateNodeComponent
  with ZkClientComponent
  with ZookeeperConfigurationComponent
  with ConfigurationProviderComponent
  with ZkCollectionStateNodeComponent
{

  "ZkNode.Create(data)" should "Create the node with data" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    val data = MyConfig("teststring")
    await(config.create(data))
    assert(await(config.getData()).get.s == "teststring")
  }

  it should "call postCreate" in async {
    var postCreateCalled = false
    val data = MyConfig("teststring")
    val root = new TestRoot()
    val configNode = new TestConfigNodeWithPostCreate("testc", root, () => {
      postCreateCalled = true
      Future.successful(())
    })
    await(configNode.create(data))
    assert(postCreateCalled)
  }

  it should "be possible to call create twice with equal data" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    val data = MyConfig("teststring")
    await(config.create(data))
    await(config.create(data))
    assert(await(config.getData()).get.s == "teststring")
  }


  it should "fail if create is called twice with different data" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    val data1 = MyConfig("teststring")
    val data2 = MyConfig("testotherstring")
    await(config.create(data1))
    assert(await(config.create(data2).failed.map(_ => true)))
  }

  "ZkNode.GetData" should "return a deserialized version of a case class" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    val data = MyConfig("teststring")
    await(config.create(data))
    assert(await(config.getData()).get.s == "teststring")
  }

  it should "return empty if no data was saved" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    await(config.create())
    assert(await(config.getData()).isEmpty)
  }

  "ZkNode.SetData" should "overwrite the data on a node" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    val data = MyConfig("teststring")
    await(config.create(data))
    config.setData(MyConfig("modifiedData"))
    assert(await(config.getData()).get.s == "modifiedData")
  }


  "ZkNode.GetChild" should "return a strongly typed child" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    val child = config.getChild[Boolean]("anotherChild")
    await(child.create(true))
    assert(await(config.getChild[Boolean]("anotherChild").getData()).get)
  }

  /*
  it should "throw an exception if the child is retrieved with the wrong type" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    val child = config.getChild[String]("childschild")
    await(child.create("hi"))
    val data = Await.result(config.getChild[Boolean]("childschild").getData(), Duration(100, MILLISECONDS))
    assertThrows[ClassCastException](data.get)
  }
  */

  "ZkNode.AwaitCondition" should "return a future that resolves when the condition evaluates to true" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    await(config.create(MyConfig("initialString")))
    val f = config.awaitCondition(o => o.s == "expectedString").map(_ => true)
    await(f.assertTimeout())
    config.setData(MyConfig("someOtherString"))
    await(f.assertTimeout())
    config.setData(MyConfig("expectedString"))
    assert(await(f))
  }

  it should "throw an exception if the node does not exist" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    val f = config.awaitCondition(o => o.s == "expectedString")
    assert(await(f.failed.map(_ => true)))
  }

  it should "fail when the node is removed" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    await(config.create(MyConfig("initialString")))
    val f = config.awaitCondition(o => o.s == "expectedString").map(_ => true)
    await(config.delete())
    assert(await(f.failed.map(_ => true)))
  }

  it should "resolve immediately if the condition was true from the beginning" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    await(config.create(MyConfig("initialString")))
    val f = config.awaitCondition(o => o.s == "initialString").map(_ => true)
    assert(await(f))
  }

  "ZkNode.ObserveData()" should "return an observable that fires when the data is modified" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    await(config.create(MyConfig("initialString")))

    val f = config.observeData().subscribeUntil(o => o.s == "awaitedstring").map(_ => true)
    await(f.assertTimeout())
    config.setData(MyConfig("someotherstring"))
    await(f.assertTimeout())
    config.setData(MyConfig("awaitedstring"))
    assert(await(f))
  }

  it should "throw an exception if the node does not exist" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    assert(await(config.observeData().awaitError().map(_ => true)))
  }

  it should "call oncompleted when the node is removed" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    await(config.create(MyConfig("initialString")))
    val f = config.observeData().subscribeUntil(_=>false).map(_ => true)
    await(f.assertTimeout())
    await(config.delete())
    assert(await(f))
  }

  class TestRoot extends ZkNodeBaseImpl("TestRoot") {
    override def parent(): ZkNodeBase = null

    override def path(): String = s"/$name"
  }

  override lazy val zookeeperConfiguration: ZookeeperConfiguration = libraryServices.zookeeperConfiguration
  override lazy val configurationProvider: ConfigurationProvider = libraryServices.configurationProvider



  class TestConfigNode(name: String, parent: ZkNodeBase) extends ZkNodeImpl[MyConfig](name, parent)

  class TestConfigNodeWithPostCreate(name: String, parent: ZkNodeBase, pc: () => Future[Unit]) extends ZkNodeImpl[MyConfig](name, parent) {
    override def postCreate(): Future[Unit] = pc()
  }

  /**
    * After each test, make sure to clean the zookeeper store
    */
  override def afterEach(): Unit = {
    Await.ready(zkClient.deleteRecursive("/"), Duration(1, SECONDS))
  }
}

case class MyConfig(s: String)
