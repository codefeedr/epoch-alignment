package org.codefeedr.core.library.internal.zookeeper

import org.codefeedr.core.library.internal.serialisation.{GenericDeserialiser, GenericSerialiser}
import rx.lang.scala.Observable

import scala.reflect.ClassTag

trait ZkQueueNode[TNode, TElement] extends ZkNode[TNode] {

  implicit def tag: ClassTag[TElement]

  val queue: ZkQueue

  /**
    * Pushes the given data element on the queue
    *
    * @param element
    */
  def push(element: TElement)

  /**
    * Observe on the queue as an observable
    *
    * @return
    */
  def observe(): Observable[TElement]
}

trait ZkQueueNodeComponent extends ZkNodeComponent { this: ZkClientComponent =>

  /**
    * Implementation of a queue around a zknode
    *
    * @tparam TNode    type of the node this trait is placed on
    * @tparam TElement type of the elemenet of the queue
    */
  trait ZkQueueNodeImpl[TNode, TElement] extends ZkNode[TNode] with ZkQueueNode[TNode, TElement] {

    /**
      * Queue toolbox
      */
    @transient override lazy val queue = new ZkQueue(zkClient, zkClient.prependPath(path()))

    /**
      * Pushes the given data element on the queue
      *
      * @param element
      */
    override def push(element: TElement) = queue.push(GenericSerialiser[TElement](element))

    /**
      * Observe on the queue as an observable
      *
      * @return
      */
    override def observe(): Observable[TElement] =
      queue.observe().map(GenericDeserialiser[TElement])
  }

}
