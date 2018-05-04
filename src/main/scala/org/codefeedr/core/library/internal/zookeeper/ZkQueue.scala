package org.codefeedr.core.library.internal.zookeeper

import java.util

import org.apache.zookeeper.AsyncCallback.ChildrenCallback
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher}
import org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE
import rx.lang.scala._
import scala.collection.JavaConverters._

/**
  * Object wrapping queue functionality around a node of the given name
  *
  * @param zkClient zookeeper client utilities
  * @param path prepended path to the node (absolute zookeeper path)
  */
class ZkQueue(zkClient: ZkClient, path: String) {
  protected lazy val connector = zkClient.getConnector()

  /**
    * Pushes the given byte array on the queue
    * @param data
    */
  def push(data: Array[Byte]): Unit = {
    connector.create(s"$path/queue-", data, OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL)
  }

  /**
    * Observe on a queue
    * TODO: Currently uses synchronous implementation to obtain the data of the element on the queue
    * @return
    */
  def observe(): Observable[Array[Byte]] = {
    @volatile var lastChild = -1
    Observable(subscriber => {
      val cb = new ChildrenCallback {
        override def processResult(rc: Int,
                                   path: String,
                                   ctx: scala.Any,
                                   children: util.List[String]): Unit = {
          children.asScala
            .sortBy(getIndex)
            .foreach(o => {
              val index = getIndex(o)
              if (lastChild < index) {
                lastChild = index
                subscriber.onNext(getChildData(o))
              }
            })
        }
      }
      val deleteCb = () => subscriber.onCompleted()
      connector.getChildren(path, getRecursiveChildWatcher(path, cb, deleteCb), cb, None)
    })
  }

  /**
    * Synchronously obtains the child data
    * @param child
    * @return
    */
  private def getChildData(child: String): Array[Byte] =
    connector.getData(s"$path/$child", false, null)

  private def getIndex(child: String): Int = child.split("-").last.toInt

  /**
    * Gets a recursive childwatcher that calls the callback whenever something changes on the children
    * @param p path to watch
    * @param cb callback to call
    * @return the watcher
    */
  def getRecursiveChildWatcher(p: String, cb: ChildrenCallback, cbDelete: () => Unit): Watcher =
    new Watcher {
      override def process(event: WatchedEvent): Unit = {
        event.getType match {
          case EventType.NodeDeleted => cbDelete()
          case EventType.NodeChildrenChanged =>
            connector.getChildren(p, getRecursiveChildWatcher(p, cb, cbDelete), cb, None)
          case _ => throw new Exception(s"Got unimplemented event: ${event.getType}")
        }
      }
    }

}
