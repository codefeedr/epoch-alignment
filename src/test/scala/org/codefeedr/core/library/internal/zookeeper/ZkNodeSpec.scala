package org.codefeedr.core.library.internal.zookeeper

import com.typesafe.scalalogging.LazyLogging
import org.apache.zookeeper.KeeperException.NoNodeException
import org.codefeedr.core.LibraryServiceSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers}
import org.codefeedr.util.observableExtension._

import scala.async.Async.{async, await}
import scala.concurrent.{Await, Future}
import org.codefeedr.util.futureExtensions._

import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}


/**
  * Testclass for  [[ZkNode]]
  */
class ZkNodeSpec  extends LibraryServiceSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with LazyLogging{

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
      Future.successful()
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

  /*
  it should "fail if create is called twice with different data" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    val data1 = MyConfig("teststring")
    val data2 = MyConfig("testotherstring")
    await(config.Create(data1))
   // await(config.Create(data2))
    assert(await(config.Create(data2).failed.map(_ => true)))
    //assert(await(config.GetData()).get.s == "teststring")
  }
  */

  "ZkNode.GetOrCreate" should "use the factory to construct a node that does not exist" in async {
    val root = new TestRoot()
    val child = new TestConfigNode("child", root)
    val config = await(child.getOrCreate(() => MyConfig("factorystring")))
    assert(await(child.exists()))
    assert(config.s == "factorystring")
    assert(await(child.getData()).get.s == "factorystring")
  }

  it should "keep the existing node there if it already exists" in async {
    val root = new TestRoot()
    val child = new TestConfigNode("child", root)
    await(child.create(MyConfig("initstring")))
    val config = await(child.getOrCreate(() => MyConfig("factorystring")))
    assert(await(child.exists()))
    assert(config.s == "initstring")
    assert(await(child.getData()).get.s == "initstring")
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

  it should "throw an exception if the child is constructed with the wrong type" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    val child = config.getChild[String]("childschild")
    await(child.create("hi"))
    val data = Await.result(config.getChild[Boolean]("childschild").getData(), Duration(100, MILLISECONDS))
    assertThrows[ClassCastException](data.get)
  }

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

  /**
    * After each test, make sure to clean the zookeeper store
    */
  override def afterEach(): Unit = {
    Await.ready(zkClient.deleteRecursive("/"), Duration(1, SECONDS))
  }
}


class TestConfigNode(name: String, parent: ZkNodeBase) extends ZkNode[MyConfig](name, parent)

class TestConfigNodeWithPostCreate(name: String, parent: ZkNodeBase, pc: () => Future[Unit]) extends ZkNode[MyConfig](name, parent) {
  override def postCreate(): Future[Unit] = pc()
}