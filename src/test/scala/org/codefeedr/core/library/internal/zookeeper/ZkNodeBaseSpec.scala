package org.codefeedr.core.library.internal.zookeeper

import org.codefeedr.core.LibraryServiceSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

import scala.async.Async.{async, await}
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}
import scala.concurrent.{Await, Future, TimeoutException}
import org.codefeedr.util.FutureExtensions._




/**
  * Testing [[ZkNodeBase]]
  */
class ZkNodeBaseSpec extends LibraryServiceSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  "ZkNode.Create()" should "create the node without data" in async {
    val root = new TestRootBase()
    assert(!await(root.Exists()))
    await(root.Create())
    assert(await(root.Exists()))
  }

  it should "create parent nodes if they do not exist" in async {
    val root = new TestRootBase()
    val config = new TestConfigNodeBase("testc", root)
    await(config.Create())
    assert(await(config.Exists()))
    assert(await(root.Exists()))
  }

  it should "call postCreate" in async {
    var postCreateCalled = false
    val root = new TestRootBase()
    val config = new TestConfigNodeWithPostCreateBase("testc", root, () => {
      postCreateCalled = true
      Future.successful()
    })
    await(config.Create())
    assert(postCreateCalled)
  }



  "ZkNode.Exists()" should "return false if a node does not exist" in async {
    val root = new TestRootBase()
    assert(!await(root.Exists()))
  }

  it should "return true if a node does exist" in async {
    val root = new TestRootBase()
    await(root.Create())
    assert(await(root.Exists()))
  }

  "ZkNode.Delete()" should "delete the node" in async {
    val root = new TestRootBase()
    await(root.Create())
    assert(await(root.Exists()))
    await(root.Delete())
    assert(!await(root.Exists()))
  }

  it should "do nothing if the node does not exist" in async {
    val root = new TestRootBase()
    await(root.Delete())
    assert(!await(root.Exists()))
  }

  it should "raise an exception if the node has children" in async {
    val root = new TestRootBase()
    val child = new TestConfigNodeBase("child", root)
    await(child.Create())
    assert(await(root.Delete().failed.map(_ => true)))
  }

  "ZkNode.DeleteRecursive" should "delete a node and its children" in async {
    val root = new TestRootBase()
    val child = new TestConfigNodeBase("child", root)
    await(child.Create())
    await(root.DeleteRecursive())
    assert(!await(root.Exists()))
    assert(!await(child.Exists()))
  }

  it should "do nothing if it does not exist" in async {
    val root = new TestRootBase()
    await(root.DeleteRecursive())
    assert(!await(root.Exists()))
  }


  "ZkNode.AwaitChild(name)" should "return a future that does not resolve if no child is created" in async {
    val root = new TestRootBase()
    await(root.Create())
    await(root.AwaitChild("child").AssertTimeout())
  }

  it should "resolve when the child already exists" in async {
    val root = new TestRootBase()
    val child = new TestConfigNodeBase("child", root)
    await(child.Create())
    assert(await(root.AwaitChild("child").map(_ => true)))
  }

  it should "resolve when the child is created" in async {
    val root = new TestRootBase()
    await(root.Create())
    val child = new TestConfigNodeBase("child", root)
    val f = root.AwaitChild("child").map(_ => true)
    await(f.AssertTimeout())
    await(child.Create())
    assert(await(f))
  }

  "ZkNode.AwaitRemoval" should "return a future that does not resolve until the node is removed" in async {
    val root = new TestRootBase()
    await(root.Create())
    val f = root.AwaitRemoval().map(_ => true)
    await(f.AssertTimeout())
    await(root.Delete())
    assert(await(f))
  }

  it should "return immediately if the node does not exist" in async {
    val root = new TestRootBase()
    val f = root.AwaitRemoval().map(_ => true)
    assert(await(f))
  }

  /**
    * After each test, make sure to clean the zookeeper store
    */
  override def afterEach(): Unit = {
    Await.ready(zkClient.DeleteRecursive("/"), Duration(1, SECONDS))
  }
}


class TestRootBase extends ZkNodeBase("TestRoot") {
  override def Parent(): ZkNodeBase = null
  override def Path(): String = s"/$name"
}

class TestConfigNodeBase(name: String, parent: ZkNodeBase) extends ZkNodeBase(name) {
  override def Parent(): ZkNodeBase = parent
}

class TestConfigNodeWithPostCreateBase(name: String, parent: ZkNodeBase, postCreate: () => Future[Unit]) extends ZkNode(name, parent) {
  override def PostCreate(): Future[Unit] = postCreate()
}
