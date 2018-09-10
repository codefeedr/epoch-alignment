package org.codefeedr.core.library.internal.zookeeper

import org.codefeedr.configuration.{ConfigurationProvider, ConfigurationProviderComponent, ZookeeperConfiguration, ZookeeperConfigurationComponent}
import org.codefeedr.core.LibraryServiceSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

import scala.async.Async.{async, await}
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}
import scala.concurrent.{Await, Future, TimeoutException}
import org.codefeedr.util.futureExtensions._




/**
  * Testing [[ZkNodeBase]]
  */
class ZkNodeBaseSpec extends LibraryServiceSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll

  with ZkStateNodeComponent
  with ZkClientComponent
  with ZookeeperConfigurationComponent
  with ConfigurationProviderComponent
{



  "ZkNode.Create()" should "create the node without data" in async {
    val root = new TestRootBase()
    assert(!await(root.exists()))
    await(root.create())
    assert(await(root.exists()))
  }

  it should "create parent nodes if they do not exist" in async {
    val root = new TestRootBase()
    val config = new TestConfigNodeBase("testc", root)
    await(config.create())
    assert(await(config.exists()))
    assert(await(root.exists()))
  }

  it should "call postCreate" in async {
    var postCreateCalled = false
    val root = new TestRootBase()
    val config = new TestConfigNodeWithPostCreateBase("testc", root, () => {
      postCreateCalled = true
      Future.successful(())
    })
    await(config.create())
    assert(postCreateCalled)
  }



  "ZkNode.Exists()" should "return false if a node does not exist" in async {
    val root = new TestRootBase()
    assert(!await(root.exists()))
  }

  it should "return true if a node does exist" in async {
    val root = new TestRootBase()
    await(root.create())
    assert(await(root.exists()))
  }

  "ZkNode.Delete()" should "delete the node" in async {
    val root = new TestRootBase()
    await(root.create())
    assert(await(root.exists()))
    await(root.delete())
    assert(!await(root.exists()))
  }

  it should "do nothing if the node does not exist" in async {
    val root = new TestRootBase()
    await(root.delete())
    assert(!await(root.exists()))
  }

  it should "raise an exception if the node has children" in async {
    val root = new TestRootBase()
    val child = new TestConfigNodeBase("child", root)
    await(child.create())
    assert(await(root.delete().failed.map(_ => true)))
  }

  "ZkNode.DeleteRecursive" should "delete a node and its children" in async {
    val root = new TestRootBase()
    val child = new TestConfigNodeBase("child", root)
    await(child.create())
    await(root.deleteRecursive())
    assert(!await(root.exists()))
    assert(!await(child.exists()))
  }

  it should "do nothing if it does not exist" in async {
    val root = new TestRootBase()
    await(root.deleteRecursive())
    assert(!await(root.exists()))
  }


  "ZkNode.AwaitChild(name)" should "return a future that does not resolve if no child is created" in async {
    val root = new TestRootBase()
    await(root.create())
    await(root.awaitChild("child").assertTimeout())
  }

  it should "resolve when the child already exists" in async {
    val root = new TestRootBase()
    val child = new TestConfigNodeBase("child", root)
    await(child.create())
    assert(await(root.awaitChild("child").map(_ => true)))
  }

  it should "resolve when the child is created" in async {
    val root = new TestRootBase()
    await(root.create())
    val child = new TestConfigNodeBase("child", root)
    val f = root.awaitChild("child").map(_ => true)
    await(f.assertTimeout())
    await(child.create())
    assert(await(f))
  }

  "ZkNode.AwaitRemoval" should "return a future that does not resolve until the node is removed" in async {
    val root = new TestRootBase()
    await(root.create())
    val f = root.awaitRemoval().map(_ => true)
    await(f.assertTimeout())
    await(root.delete())
    assert(await(f))
  }

  it should "return immediately if the node does not exist" in async {
    val root = new TestRootBase()
    val f = root.awaitRemoval().map(_ => true)
    assert(await(f))
  }

  /**
    * After each test, make sure to clean the zookeeper store
    */
  override def afterEach(): Unit = {
    super.afterEach()
    Await.ready(zkClient.deleteRecursive("/"), Duration(1, SECONDS))
  }


  override val zookeeperConfiguration: ZookeeperConfiguration = libraryServices.zookeeperConfiguration
  override val configurationProvider: ConfigurationProvider = libraryServices.configurationProvider

  class TestRootBase extends ZkNodeBaseImpl("TestRoot") {
    override def parent(): ZkNodeBase = null
    override def path(): String = s"/$name"
  }

  class TestConfigNodeBase(name: String, p: ZkNodeBase) extends ZkNodeBaseImpl(name) {
    override def parent(): ZkNodeBase = p
  }

  class TestConfigNodeWithPostCreateBase(name: String, parent: ZkNodeBase, pc: () => Future[Unit]) extends ZkNodeImpl(name, parent) {
    override def postCreate(): Future[Unit] = pc()
  }



}

