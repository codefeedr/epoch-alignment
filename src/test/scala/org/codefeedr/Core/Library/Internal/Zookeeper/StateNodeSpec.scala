package org.codefeedr.Core.Library.Internal.Zookeeper

import com.typesafe.scalalogging.LazyLogging
import org.apache.zookeeper.KeeperException.NoNodeException
import org.codefeedr.Core.LibraryServiceSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

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
    val stateNode = child.GetStateNode()
    assert(stateNode.parent == child)
    assert(stateNode.name == "state")
  }

  "ZkStateNode.GetState" should "return the current data stored in the state" in async {
    val root = new TestRoot()
    val child = new TestStateNode("child", root)
    await(child.Create())
    assert(await(child.GetState()).get == "initialvalue")
  }

  it should "throw an exception if the node was not created" in async {
    val root = new TestRoot()
    val child = new TestStateNode("child", root)
    assert(await(child.GetState().failed.map(_ => true)))
  }

  "ZkStateNode.SetState(data)" should "set the data on the state" in async {
    val root = new TestRoot()
    val child = new TestStateNode("child", root)
    await(child.Create())
    await(child.SetState("samplestate"))
    assert(await(child.GetState()).get == "samplestate")
  }

  it should "throw an exception if the node was not created" in async {
    val root = new TestRoot()
    val child = new TestStateNode("child", root)
    assert(await(child.SetState("samplestate").failed.map(_ => true)))
  }


  "ZkStateNode.WatchState(c)" should "return a future that resolves when the given condition evaluates to true" in async {
    val root = new TestRoot()
    val child = new TestStateNode("child", root)
    child.Create()
    val f = child.WatchState(a => a == "expected")
    assertThrows[TimeoutException](Await.ready(f, Duration(100, MILLISECONDS)))
    child.SetState("notexpected")
    assertThrows[TimeoutException](Await.ready(f, Duration(100, MILLISECONDS)))
    child.SetState("expected")
    assert(await(f.map(_ => true)))
  }


  it should "fail if the node is removed" in async {
    val root = new TestRoot()
    val child = new TestStateNode("child", root)
    await(child.Create())
    val f = child.WatchState(a => a == "expected")
    assertThrows[TimeoutException](Await.ready(f, Duration(100, MILLISECONDS)))
    await(child.Delete())
    assert(await(f.failed.map(_ => true)))
  }

  it should "fail if the node does not exist" in async {
    val root = new TestRoot()
    val child = new TestStateNode("child", root)
    val f = child.WatchState(a => a == "expected")
    assert(await(f.failed.map(_ => true)))
  }



  /**
    * After each test, make sure to clean the zookeeper store
    */
  override def afterEach(): Unit = {
    Await.ready(zkClient.DeleteRecursive("/"), Duration(1, SECONDS))
  }
}


case class MyConfig(s: String)

class TestRoot extends ZkNodeBase("TestRoot") {
  override def Parent(): ZkNodeBase = null
  override def Path(): String = s"/$name"
}

class TestStateNode(name: String, parent: ZkNodeBase) extends ZkNode[MyConfig](name, parent) with ZkStateNode[MyConfig,String] {
  /**
    * The base class needs to expose the typeTag, no typeTag constraints can be put on traits
    *
    * @return
    */
  override def TypeT() : ClassTag[String] = ClassTag(classOf[String])

  override def InitialState(): String = "initialvalue"
}

