package org.codefeedr.Core.Library.Internal.Zookeeper

import com.typesafe.scalalogging.LazyLogging
import org.apache.zookeeper.KeeperException.NoNodeException
import org.codefeedr.Core.LibraryServiceSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers}
import org.codefeedr.util.ObservableExtension._

import scala.async.Async.{async, await}
import scala.concurrent.{Await, Future}
import org.codefeedr.Util.FutureExtensions._

import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}


/**
  * Testclass for  [[ZkNode]]
  */
class ZkNodeSpec  extends LibraryServiceSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with LazyLogging{

  "ZkNode.Create(data)" should "Create the node with data" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    val data = MyConfig("teststring")
    await(config.Create(data))
    assert(await(config.GetData()).get.s == "teststring")
  }

  it should "call postCreate" in async {
    var postCreateCalled = false
    val data = MyConfig("teststring")
    val root = new TestRoot()
    val configNode = new TestConfigNodeWithPostCreate("testc", root, () => {
      postCreateCalled = true
      Future.successful()
    })
    await(configNode.Create(data))
    assert(postCreateCalled)
  }

  it should "be possible to call create twice with equal data" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    val data = MyConfig("teststring")
    await(config.Create(data))
    await(config.Create(data))
    assert(await(config.GetData()).get.s == "teststring")
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
    val config = await(child.GetOrCreate(() => MyConfig("factorystring")))
    assert(await(child.Exists()))
    assert(config.s == "factorystring")
    assert(await(child.GetData()).get.s == "factorystring")
  }

  it should "keep the existing node there if it already exists" in async {
    val root = new TestRoot()
    val child = new TestConfigNode("child", root)
    await(child.Create(MyConfig("initstring")))
    val config = await(child.GetOrCreate(() => MyConfig("factorystring")))
    assert(await(child.Exists()))
    assert(config.s == "initstring")
    assert(await(child.GetData()).get.s == "initstring")
  }

  "ZkNode.GetData" should "return a deserialized version of a case class" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    val data = MyConfig("teststring")
    await(config.Create(data))
    assert(await(config.GetData()).get.s == "teststring")
  }

  it should "return empty if no data was saved" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    await(config.Create())
    assert(await(config.GetData()).isEmpty)
  }

  "ZkNode.SetData" should "overwrite the data on a node" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    val data = MyConfig("teststring")
    await(config.Create(data))
    config.SetData(MyConfig("modifiedData"))
    assert(await(config.GetData()).get.s == "modifiedData")
  }


  "ZkNode.GetChild" should "return a strongly typed child" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    val child = config.GetChild[Boolean]("anotherChild")
    await(child.Create(true))
    assert(await(config.GetChild[Boolean]("anotherChild").GetData()).get)
  }

  it should "throw an exception if the child is constructed with the wrong type" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    val child = config.GetChild[String]("childschild")
    await(child.Create("hi"))
    val data = Await.result(config.GetChild[Boolean]("childschild").GetData(), Duration(100, MILLISECONDS))
    assertThrows[ClassCastException](data.get)
  }

  "ZkNode.AwaitCondition" should "return a future that resolves when the condition evaluates to true" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    await(config.Create(MyConfig("initialString")))
    val f = config.AwaitCondition(o => o.s == "expectedString").map(_ => true)
    await(f.AssertTimeout())
    config.SetData(MyConfig("someOtherString"))
    await(f.AssertTimeout())
    config.SetData(MyConfig("expectedString"))
    assert(await(f))
  }

  it should "throw an exception if the node does not exist" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    val f = config.AwaitCondition(o => o.s == "expectedString")
    assert(await(f.failed.map(_ => true)))
  }

  it should "fail when the node is removed" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    await(config.Create(MyConfig("initialString")))
    val f = config.AwaitCondition(o => o.s == "expectedString").map(_ => true)
    await(config.Delete())
    assert(await(f.failed.map(_ => true)))
  }

  "ZkNode.ObserveData()" should "return an observable that fires when the data is modified" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    await(config.Create(MyConfig("initialString")))

    val f = config.ObserveData().SubscribeUntil(o => o.s == "awaitedstring").map(_ => true)
    await(f.AssertTimeout())
    config.SetData(MyConfig("someotherstring"))
    await(f.AssertTimeout())
    config.SetData(MyConfig("awaitedstring"))
    assert(await(f))
  }

  it should "throw an exception if the node does not exist" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    assert(await(config.ObserveData().AwaitError().map(_ => true)))
  }

  it should "call oncompleted when the node is removed" in async {
    val root = new TestRoot()
    val config = new TestConfigNode("testc", root)
    await(config.Create(MyConfig("initialString")))
    val f = config.ObserveData().SubscribeUntil(_=>false).map(_ => true)
    await(f.AssertTimeout())
    await(config.Delete())
    assert(await(f))
  }

  /**
    * After each test, make sure to clean the zookeeper store
    */
  override def afterEach(): Unit = {
    Await.ready(zkClient.DeleteRecursive("/"), Duration(1, SECONDS))
  }
}


class TestConfigNode(name: String, parent: ZkNodeBase) extends ZkNode[MyConfig](name, parent)

class TestConfigNodeWithPostCreate(name: String, parent: ZkNodeBase, postCreate: () => Future[Unit]) extends ZkNode[MyConfig](name, parent) {
  override def PostCreate(): Future[Unit] = postCreate()
}