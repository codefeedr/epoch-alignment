package org.codefeedr.core.library.internal.zookeeper

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.core.LibraryServiceSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

import scala.async.Async.{async, await}
import scala.concurrent.{Await, Future}
import org.codefeedr.util.futureExtensions._

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}
import resource.managed

/**
  * Test class for ZkReadLock
  * Please do not use this test class as example implementation, it does not properly manage locks
  * It just tests its behavior!
  */
class ZkReadLockSpec extends LibraryServiceSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with LazyLogging {
  "ZkReadLock(path)" should "resolve if no other locks are present" in async {
    val root = new TestRoot()
    await(root.create())
    val lock = root.readLock().map(_ => true)
    assert(await(lock))
  }

  it should "resolve if there are previously opened readlocks" in async {
    val root = new TestRoot()
    await(root.create())
    val existing = await(root.readLock())
    val lock = await(root.readLock())
    assert(
      managed(lock) acquireAndGet {
        l => l.isOpen}
    )
    existing.close()
    assert(!lock.isOpen)
  }


  it should "wait if there is a write lock active" in async {
    val root = new TestRoot()
    await(root.create())
    val writeLock = await(root.writeLock())
    val readLock = root.readLock()
    await(readLock.assertTimeout())
    writeLock.close()
    await(readLock.map(_ => assert(true)))
  }

  it should "resolve before later write locks" in async {
    //This test does not properly clean up locks, relies on post test cleanup
    val root = new TestRoot()
    await(root.create())
    val writeLock = await(root.writeLock())
    val readLock = root.readLock()
    await(readLock.assertTimeout())
    val writeLock2 = root.writeLock()
    await(writeLock2.assertTimeout())
    writeLock.close()
    assert(await(readLock.map(_ => true)))
  }



  /**
    * After each test, make sure to clean the zookeeper store
    */
  override def afterEach(): Unit = {
    super.afterEach()
    Await.ready(zkClient.deleteRecursive("/"), Duration(1, SECONDS))
  }

  /**
    * Also clean up on before each, during development junk might end up in zookeeper when a test crashes
    */
  override def beforeEach(): Unit  = afterEach()
}
