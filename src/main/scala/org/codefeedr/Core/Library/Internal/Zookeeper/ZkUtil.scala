

package org.codefeedr.Core.Library.Internal.Zookeeper

import com.twitter.zk.ZkClient
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE
import scala.concurrent.ExecutionContext.Implicits.global

import scala.async.Async.{async, await}
import scala.concurrent.Future
import org.codefeedr.TwitterUtils._

object ZkUtil {
  @transient private lazy val zk: ZkClient = ZookeeperConfig.getClient

  /**
    * Retrieve a boolean if the given path exists on zookeeper
    * @param path the path to search
    * @return A future that resolves to a boolean
    */
  def pathExists(path: String): Future[Boolean] =
    zk.apply().map(o => o.exists(path, false)).map(o => o != null).asScala

  /**
    * Delete the given zookeeper path and return a future when deletion is completed
    * @param path The path to delete
    * @return Future that resolves when the path has been deleted
    */
  def Delete(path: String): Future[Unit] =
    zk(path).delete(-1).map(_ => ()).asScala

  /**
    * Recursively delete a path and all its children
    * @param path the path to remove
    * @return
    */
  def DeleteRecursive(path: String): Future[Unit] = async {
    val childPaths =
      await(zk(path).getChildren.apply().map(o => o.children.map(o => o.path)).asScala)
    await(Future.sequence(childPaths.map(DeleteRecursive)))
    await(Delete(path))
  }

  /**
    * Create the given node without data
    * @param path the path to create
    * @return A future that resolves when the path has been created
    */
  def Create(path: String): Future[String] = Create(path, null)

  /**
    * Creates the given path with the given byte array as data
    * @param path the absolute path to create
    * @param data the data to store at the path
    * @return A future that resolves when the path has been created
    */
  def Create(path: String, data: Array[Byte]): Future[String] =
    zk.apply().map(o => o.create(path, data, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)).asScala

}
