package org.codefeedr.core.library.internal.zookeeper

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.core.LibraryServiceSpec
import org.codefeedr.util.futureExtensions._
import org.codefeedr.util.observableExtension._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

import scala.async.Async.{async, await}
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}


/**
  * TestClass for [[ZkCollectionNode]]
  */
class ZkCollectionNodeSpec  extends LibraryServiceSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with LazyLogging {

  "ZkCollectionNode.GetChild(name)" should "Return the ZkNode of the child of the given name, even if it does not exist" in {
    val root = new TestRoot()
    val collection = new TestCollectionNode("testCollection", root)
    val child = collection.getChild("child1")
    assert(child.name == "child1")
  }

  "ZkCollectionNode.GetChildren" should "Return a collection of all current children known in zookeeper" in async {
    val root = new TestRoot()
    val collection = new TestCollectionNode("testCollection", root)
    await(collection.create())
    await(collection.getChild("child1").create("child1data"))
    await(collection.getChild("child2").create("child2data"))
    val children = await(collection.getChildren())
    assert(children.exists(o => o.name == "child1"))
    assert(children.exists(o => o.name == "child2"))
  }

  it should "Return an empty collection when no child exists" in async {
    val root = new TestRoot()
    val collection = new TestCollectionNode("testCollection", root)
    await(collection.create())
    val children = await(collection.getChildren())
    assert(children.isEmpty)
  }


  "ZkCollectionNode.AwaitChildNode" should "return a future that resolves when the passed child is created" in async {
    val root = new TestRoot()
    val collection = new TestCollectionNode("testCollection", root)
    await(collection.create())
    val f = collection.awaitChildNode("expectedchild")
    await(f.assertTimeout())
    collection.getChild("bastardchild").create("IAmNotRecogNized")
    await(f.assertTimeout())
    collection.getChild("expectedchild").create("IGetAllHeritage")
    assert(await(f.map(_ => true)))
  }

  it should "fail when the collection is removed" in async {
    val root = new TestRoot()
    val collection = new TestCollectionNode("testCollection", root)
    await(collection.create())
    val f = collection.awaitChildNode("expectedchild").failed.map(_ => true)
    collection.delete()
    assert(await(f))
  }

  it should "also resolve if the child already exists" in async {
    val root = new TestRoot()
    val collection = new TestCollectionNode("testCollection", root)
    await(collection.create())
    await(collection.getChild("expectedchild").create("IGetAllHeritage"))
    val f = collection.awaitChildNode("expectedchild")
    assert(await(f.map(_ => true)))
  }

  "ZkCollectionNode.ObserveNewChildren" should "Create an observalbe that contains all newly created nodes and complete if node is removed" in async {
    val root = new TestRoot()
    val collection = new TestCollectionNode("testCollection", root)
    await(collection.create())
    val observable = collection.observeNewChildren()
    val data = observable.collectAsFuture()
    await(collection.getChild("child1").create("firstone"))
    await(collection.getChild("child2").create("second"))
    await(collection.deleteRecursive())
    val d = await(data)
    assert(d.exists(o => o.name == "child1"))
    assert(d.exists(o => o.name == "child2"))
  }

  /**
    * After each test, make sure to clean the zookeeper store
    */
  override def beforeEach(): Unit = {
    super.beforeEach()
    Await.ready(zkClient.deleteRecursive("/"), Duration(1, SECONDS))
  }

  /**
    * After each test, make sure to clean the zookeeper store
    */
  override def afterEach(): Unit = {
    super.afterEach()
    Await.ready(zkClient.deleteRecursive("/"), Duration(1, SECONDS))
  }
}

class TestCollectionNode(name: String, parent: ZkNodeBase)(implicit override val zkClient: ZkClient) extends ZkCollectionNode[TestCollectionChildNode,Unit](name, parent, (n, p) => new TestCollectionChildNode(n,p))

class TestCollectionChildNode(name: String, parent: ZkNodeBase)(implicit override val zkClient: ZkClient) extends ZkNode[String](name, parent)

