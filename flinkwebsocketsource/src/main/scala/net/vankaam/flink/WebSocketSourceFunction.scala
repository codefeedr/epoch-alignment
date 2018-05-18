package net.vankaam.flink


import java.util
import java.util.Collections

import scala.collection.JavaConversions._

import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

/**
  *
  * @param url
  */
class WebSocketSourceFunction(url: String) extends RichSourceFunction[String] with ListCheckpointed[java.lang.Long] with Serializable {

  /** Current position of the reader. Also state of the source */
  var offset: Long = 0

  @volatile private var isRunning = true

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {

    cleanUp()
  }

  override def cancel(): Unit = {
    isRunning = false
  }


  /** Perform cleanup after run has finished */
  private def cleanUp(): Unit = {

  }

  /** Restore offset to given point */
  override def restoreState(state: util.List[java.lang.Long]): Unit = {
    for(s <- state) {
      offset = s
    }
  }

  /** Snapshot current offset */
  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[java.lang.Long] =
    Collections.singletonList(offset)
}



