package org.codefeedr.Core.Library.Internal.Zookeeper

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.Core.LibraryServiceSpec
import org.codefeedr.Util.FutureExtensions._
import org.codefeedr.util.ObservableExtension._
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
    val child = collection.GetChild("child1")
    assert(child.name == "child1")
  }

  "ZkCollectionNode.GetChildren" should "Return a collection of all current children known in zookeeper" in async {
    val root = new TestRoot()
    val collection = new TestCollectionNode("testCollection", root)
    await(collection.Create())
    await(collection.GetChild("child1").Create("child1data"))
    await(collection.GetChild("child2").Create("child2data"))
    val children = await(collection.GetChildren())
    assert(children.exists(o => o.name == "child1"))
    assert(children.exists(o => o.name == "child2"))
  }

  it should "Return an empty collection when no child exists" in async {
    val root = new TestRoot()
    val collection = new TestCollectionNode("testCollection", root)
    await(collection.Create())
    val children = await(collection.GetChildren())
    assert(children.isEmpty)
  }


  "ZkCollectionNode.AwaitChildNode" should "return a future that resolves when the passed child is created" in async {
    val root = new TestRoot()
    val collection = new TestCollectionNode("testCollection", root)
    await(collection.Create())
    val f = collection.AwaitChildNode("expectedchild")
    await(f.AssertTimeout())
    collection.GetChild("bastardchild").Create("IAmNotRecogNized")
    await(f.AssertTimeout())
    collection.GetChild("expectedchild").Create("IGetAllHeritage")
    assert(await(f.map(_ => true)))
  }

  it should "fail when the collection is removed" in async {
    val root = new TestRoot()
    val collection = new TestCollectionNode("testCollection", root)
    await(collection.Create())
    val f = collection.AwaitChildNode("expectedchild").failed.map(_ => true)
    collection.Delete()
    assert(await(f))
  }

  it should "also resolve if the child already exists" in async {
    val root = new TestRoot()
    val collection = new TestCollectionNode("testCollection", root)
    await(collection.Create())
    await(collection.GetChild("expectedchild").Create("IGetAllHeritage"))
    val f = collection.AwaitChildNode("expectedchild")
    assert(await(f.map(_ => true)))
  }

  "ZkCollectionNode.ObserveNewChildren" should "Create an observalbe that contains all newly created nodes and complete if node is removed" in async {
    val root = new TestRoot()
    val collection = new TestCollectionNode("testCollection", root)
    await(collection.Create())
    val observable = collection.ObserveNewChildren()
    val data = observable.Collect()
    await(collection.GetChild("child1").Create("firstone"))
    await(collection.GetChild("child2").Create("second"))
    await(collection.DeleteRecursive())
    val d = await(data)
    assert(d.exists(o => o.name == "child1"))
    assert(d.exists(o => o.name == "child2"))
  }


  /**
    * After each test, make sure to clean the zookeeper store
    */
  override def afterEach(): Unit = {
    Await.ready(zkClient.DeleteRecursive("/"), Duration(1, SECONDS))
  }
}

class TestCollectionNode(name: String, parent: ZkNodeBase) extends ZkCollectionNode[TestCollectionChildNode](name, parent, (n, p) => new TestCollectionChildNode(n,p))

class TestCollectionChildNode(name: String, parent: ZkNodeBase) extends ZkNode[String](name, parent)

