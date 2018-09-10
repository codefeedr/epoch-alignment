package org.codefeedr.core.library.internal.zookeeper

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.configuration.{ConfigurationProvider, ConfigurationProviderComponent, ZookeeperConfiguration, ZookeeperConfigurationComponent}
import org.codefeedr.core.LibraryServiceSpec
import org.codefeedr.util.futureExtensions._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers}
import resource.managed

import scala.async.Async.{async, await}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.{Duration, SECONDS}

/**
  * Test class for ZkReadLock
  * Please do not use this test class as example implementation, it does not properly manage locks
  * It just tests its behavior!
  */
class ZkWriteLockSpec extends LibraryServiceSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with LazyLogging
  with ZkStateNodeComponent
  with ZkClientComponent
  with ZookeeperConfigurationComponent
  with ConfigurationProviderComponent
{
  "ZkWriteLock(path)" should "Succeed if no other locks are present" in async {
    val root = new TestRoot()
    await(root.create())
    val lock = root.writeLock().map(_ => true)
    assert(await(lock))
  }

  it should "Wait until a read lock has been closed" in async {
    val root = new TestRoot()
    await(root.create())
    val existing = await(root.readLock())
    val writeLock = root.writeLock().map(_ => true)
    await(writeLock.assertTimeout())
    existing.close()
    assert(await(writeLock))
  }

  it should "Wait until a previous write lock has been closed" in async {
    val root = new TestRoot()
    await(root.create())
    val existing = await(root.writeLock())
    val writeLock = root.writeLock().map(_ => true)
    await(writeLock.assertTimeout())
    existing.close()
    assert(await(writeLock))
  }


  it should "Wait for multiple read locks" in async {
    val root = new TestRoot()
    await(root.create())
    val existing1 = await(root.readLock())
    val existing2 = await(root.readLock())
    val writeLock = root.writeLock().map(_ => true)
    await(writeLock.assertTimeout())
    existing1.close()
    await(writeLock.assertTimeout())
    existing2.close()
    assert(await(writeLock))
  }

  it should "resolve before later read locks" in async {
    //This test does not properly clean up locks, relies on post test cleanup
    val root = new TestRoot()
    await(root.create())
    val readLock1 = await(root.readLock())
    val writeLock = root.writeLock()
    await(writeLock.assertTimeout())
    val readLock2 = root.readLock()
    await(readLock2.assertTimeout())
    readLock1.close()
    assert(await(writeLock.map(_ => true)))
    await(readLock2.assertTimeout())
  }

  class TestRoot extends ZkNodeBaseImpl("TestRoot") {
    override def parent(): ZkNodeBase = null

    override def path(): String = s"/$name"
  }
  override val zookeeperConfiguration: ZookeeperConfiguration = libraryServices.zookeeperConfiguration
  override val configurationProvider: ConfigurationProvider = libraryServices.configurationProvider

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
