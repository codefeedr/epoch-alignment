package org.codefeedr.core.library.internal.zookeeper

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.core.LibraryServiceSpec
import org.codefeedr.util.futureExtensions._
import org.codefeedr.util.observableExtension._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

import scala.async.Async.{async, await}
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}
import scala.reflect.ClassTag


/**
  * Testclass for  [[ZkCollectionStateNode]]
  */
class ZkCollectionStateNodeSpec  extends LibraryServiceSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with LazyLogging {

  "ZkCollectionStateNode.GetState" should "return the aggregate of all states" in async {
    val root = new TestRoot()
    val collection = new TestCollectionStateNode("testCollection", root)
    await(collection.create())
    await(collection.getChild("child1").create("child1data"))
    await(collection.getChild("child2").create("child2data"))
    val aggregate = await(collection.getState())
    assert(aggregate == "(s_child1)-(s_child2)" || aggregate == "(s_child2)-(s_child1)")
  }

  it should "return the default if there are no children" in async {
    val root = new TestRoot()
    val collection = new TestCollectionStateNode("testCollection", root)
    await(collection.create())
    val aggregate = await(collection.getState())
    assert(aggregate == null)
  }

  it should "throw an exception if the collection does not exist" in async {
    val root = new TestRoot()
    val collection = new TestCollectionStateNode("testCollection", root)
    assert(await(collection.getState().failed.map(_ => true)))
  }

  "ZkCollectionStateNode.WatchStateAggregate" should "place a watch that resolves when the condition evaluates to true for all states" in async {
    val root = new TestRoot()
    val collection = new TestCollectionStateNode("testCollection", root)
    await(collection.create())
    val c1 = collection.getChild("child1")
    val c2 = collection.getChild("child2")
    await(c1.create("child1data"))
    await(c2.create("child2data"))

    val f = collection.watchStateAggregate(s => s == "flagged").map(_ => true)
    await(f.assertTimeout())
    c1.setState("flagged")
    await(f.assertTimeout())
    c2.setState("flagged")

    assert(await(f))
  }

  it should "listen for new children" in async {
    val root = new TestRoot()
    val collection = new TestCollectionStateNode("testCollection", root)
    await(collection.create())
    val c1 = collection.getChild("child1")
    val c2 = collection.getChild("child2")
    val c3 = collection.getChild("child3")
    await(c1.create("child1data"))
    await(c2.create("child2data"))

    val f = collection.watchStateAggregate(s => s == "flagged").map(_ => true)
    await(f.assertTimeout())
    c1.setState("flagged")
    await(f.assertTimeout())
    c3.create("child3data")
    await(f.assertTimeout())
    c2.setState("flagged")
    await(f.assertTimeout())
    c3.setState("flagged")
    assert(await(f))
  }

  it should "no longer accept if a state is no longer valid" in async {
    val root = new TestRoot()
    val collection = new TestCollectionStateNode("testCollection", root)
    await(collection.create())
    val c1 = collection.getChild("child1")
    val c2 = collection.getChild("child2")
    val c3 = collection.getChild("child3")
    await(c1.create("child1data"))
    await(c2.create("child2data"))
    await(c3.create("child3data"))

    val f = collection.watchStateAggregate(s => s == "flagged").map(_ => true)
    await(f.assertTimeout())
    c1.setState("flagged")
    await(f.assertTimeout())
    c2.setState("flagged")
    await(f.assertTimeout())
    c1.setState("nolongerflagged")
    await(f.assertTimeout())
    c3.setState("flagged")
    await(f.assertTimeout())
    c1.setState("flagged")
    assert(await(f))
  }

  it should "no longer watch for state nodes that no longer exist" in async {
    val root = new TestRoot()
    val collection = new TestCollectionStateNode("testCollection", root)
    await(collection.create())
    val c1 = collection.getChild("child1")
    val c2 = collection.getChild("child2")
    val c3 = collection.getChild("child3")
    await(c1.create("child1data"))
    await(c2.create("child2data"))
    await(c3.create("child3data"))

    val f = collection.watchStateAggregate(s => s == "flagged").map(_ => true)
    await(f.assertTimeout())
    c1.setState("flagged")
    await(f.assertTimeout())
    c2.delete()
    await(f.assertTimeout())
    c3.setState("flagged")
    assert(await(f))
  }

  it should "also trigger if the removal of a state node moves the evaluation to true" in async {
    val root = new TestRoot()
    val collection = new TestCollectionStateNode("testCollection", root)
    await(collection.create())
    val c1 = collection.getChild("child1")
    val c2 = collection.getChild("child2")
    val c3 = collection.getChild("child3")
    await(c1.create("child1data"))
    await(c2.create("child2data"))
    await(c3.create("child3data"))

    val f = collection.watchStateAggregate(s => s == "flagged").map(_ => true)
    await(f.assertTimeout())
    c1.setState("flagged")
    await(f.assertTimeout())
    c3.setState("flagged")
    await(f.assertTimeout())
    c2.delete()
    assert(await(f))
  }

  it should "return immediately if the conditions are already met" in async {
    val root = new TestRoot()
    val collection = new TestCollectionStateNode("testCollection", root)
    await(collection.create())
    val c1 = collection.getChild("child1")
    await(c1.create("child1data"))
    await(c1.setState("flagged"))
    val f = collection.watchStateAggregate(s => s == "flagged").map(_ => true)
    assert(await(f))
  }

  /**
    * After each test, make sure to clean the zookeeper store
    */
  override def afterEach(): Unit = {
    Await.ready(zkClient.deleteRecursive("/"), Duration(1, SECONDS))
  }
}



class TestCollectionStateNode(name: String, parent: ZkNodeBase)
  extends ZkCollectionNode[TestCollectionStateChildNode,Unit](name, parent, (n, p) => new TestCollectionStateChildNode(n,p))
  with ZkCollectionStateNode[TestCollectionStateChildNode,Unit,String, String,String] {
  /**
    * Initial value of the aggreagate state before the fold
    *
    * @return
    */
  override def initial(): String = null

  /**
    * Mapping from the child to the aggregate state
    *
    * @param child
    * @return
    */
  override def mapChild(child: String): String = child

  /**
    * Reduce operator of the aggregation
    *
    * @param left
    * @param right
    * @return
    */
  override def reduceAggregate(left: String, right: String): String = {
    if(left != null) {
      s"$left-$right"
    } else {
      right
    }
  }
}

class TestCollectionStateChildNode(name: String, parent: ZkNodeBase)
  extends ZkNode[String](name, parent)
  with ZkStateNode[String,String] {
  /**
    * The base class needs to expose the typeTag, no typeTag constraints can be put on traits
    *
    * @return
    */
  override def typeT(): ClassTag[String] = ClassTag(classOf[String])

  /**
    * The initial state of the node. State is not allowed to be empty
    *
    * @return
    */
  override def initialState(): String = s"(s_$name)"
}
