package org.codefeedr.core.library.internal.zookeeper

import com.typesafe.scalalogging.LazyLogging
import org.apache.zookeeper.KeeperException.NoNodeException
import org.codefeedr.core.LibraryServiceSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers}
import org.codefeedr.util.futureExtensions.AssertableFuture

import scala.async.Async.{async, await}
import scala.concurrent.{Await, Future, TimeoutException}
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}

import scala.reflect.ClassTag

/**
  * Testclass for  [[ZkStateNode]]
  */
class ZkStateNodeSpec  extends LibraryServiceSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with LazyLogging{
  "ZkStateNode.GetStateNode()" should "return the ckNode representing the state" in async {
    val root = new TestRoot()
    val child = new TestStateNode("child", root)
    val stateNode = child.getStateNode()
    assert(stateNode.parent == child)
    assert(stateNode.name == "state")
  }

  "ZkStateNode.GetState" should "return the current data stored in the state" in async {
    val root = new TestRoot()
    val child = new TestStateNode("child", root)
    await(child.create())
    assert(await(child.getState()).get == "initialvalue")
  }

  it should "throw an exception if the node was not created" in async {
    val root = new TestRoot()
    val child = new TestStateNode("child", root)
    assert(await(child.getState().failed.map(_ => true)))
  }

  "ZkStateNode.SetState(data)" should "set the data on the state" in async {
    val root = new TestRoot()
    val child = new TestStateNode("child", root)
    await(child.create())
    await(child.setState("samplestate"))
    assert(await(child.getState()).get == "samplestate")
  }

  it should "throw an exception if the node was not created" in async {
    val root = new TestRoot()
    val child = new TestStateNode("child", root)
    assert(await(child.setState("samplestate").failed.map(_ => true)))
  }


  "ZkStateNode.WatchState(c)" should "return a future that resolves when the given condition evaluates to true" in async {
    val root = new TestRoot()
    val child = new TestStateNode("child", root)
    await(child.create())
    val f = child.watchState(a => a == "expected")
    f.assertTimeout()
    child.setState("notexpected")
    f.assertTimeout()
    child.setState("expected")
    assert(await(f.map(_ => true)))
  }


  it should "fail if the node is removed" in async {
    val root = new TestRoot()
    val child = new TestStateNode("child", root)
    await(child.create())
    val f = child.watchState(a => a == "expected")
    f.assertTimeout()
    await(child.delete())
    assert(await(f.failed.map(_ => true)))
  }

  it should "fail if the node does not exist" in async {
    val root = new TestRoot()
    val child = new TestStateNode("child", root)
    val f = child.watchState(a => a == "expected")
    assert(await(f.failed.map(_ => true)))
  }


  /**
    * After each test, make sure to clean the zookeeper store
    */
  override def afterEach(): Unit = {
    super.afterEach()
    Await.ready(zkClient.deleteRecursive("/"), Duration(1, SECONDS))
  }
}




case class MyConfig(s: String)

class TestRoot(implicit zkClient: ZkClient) extends ZkNodeBase("TestRoot")(zkClient) {
  override def parent(): ZkNodeBase = null
  override def path(): String = s"/$name"
}

class TestStateNode(name: String, parent: ZkNodeBase)(implicit override val zkClient: ZkClient) extends ZkNode[MyConfig](name, parent) with ZkStateNode[MyConfig,String] {
  /**
    * The base class needs to expose the typeTag, no typeTag constraints can be put on traits
    *
    * @return
    */
  override def typeT() : ClassTag[String] = ClassTag(classOf[String])

  override def initialState(): String = "initialvalue"
}

