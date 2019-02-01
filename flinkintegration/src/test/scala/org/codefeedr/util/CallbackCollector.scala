package org.codefeedr.util

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.watermark.Watermark

import scala.collection.mutable

/**
  * Simple collector design to unit test sources. Collects a passed number of elements, and then calls the callback
  * @param count number of elements to collect
  * @param cb callback to call when number of elements has been reached
  * @tparam TSource type of the elements to collect
  */
class CallbackCollector[TSource](count:Int, cb:() => Unit) extends SourceFunction.SourceContext[TSource] {
  private val collectedElements:mutable.MutableList[TSource] = new mutable.MutableList[TSource]

  def getElements: List[TSource] = collectedElements.toList

  val checkpointLock = new Object()
  override def collect(element: TSource): Unit = {
    collectedElements+=element
    if(collectedElements.size == count) {
      cb()
    } else if(collectedElements.size > count) {
      throw new IllegalStateException("Collected more elements than count. Did you act on the callback properly?")
    }

  }

  override def collectWithTimestamp(element: TSource, timestamp: Long): Unit = collect(element)

  override def emitWatermark(mark: Watermark): Unit = {}

  override def markAsTemporarilyIdle(): Unit = ???

  override def getCheckpointLock: AnyRef = checkpointLock

  override def close(): Unit = ???
}
