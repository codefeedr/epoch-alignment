package org.codefeedr.core.library.internal.logging

import org.apache.flink.runtime.state.FunctionSnapshotContext
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.codefeedr.util.LazyMdcLogging
import org.joda.time.DateTime
import org.slf4j.MDC

case class CheckpointMeasurement(checkpointId: Long,
                                 offset: Long,
                                 elements: Long,
                                 latency: Long,
                                 checkpointLatency: Long)

/**
  * Checkpointed function that measures
  */
trait MeasuredCheckpointedFunction
    extends LazyMdcLogging
    with CheckpointedFunction
    with LabeledOperator {

  def getLatency: Long
  def getCurrentOffset: Long
  def getLastEventTime: Long

  private var lastOffset: Long = 0L

  private def getCurrentLatency: Long = getLatency

  protected val enableLoging: Boolean = true

  /**
    * Creates a snapshot of the current state with the passed checkpointId
    * Not a pure function, after calling ,the last offset is set to the current offset
    */
  protected def snapshotMeasurement(checkPointId: Long): CheckpointMeasurement = {
    val currentOffset = getCurrentOffset
    val measurement = CheckpointMeasurement(checkPointId,
                                            currentOffset,
                                            currentOffset - lastOffset,
                                            getCurrentLatency,if(getLastEventTime != 0) { System.currentTimeMillis() - getLastEventTime} else {0}
                                           )
    lastOffset = currentOffset
    measurement
  }

  /**
    * Method that should be called whenever the function snapshots some state
    */
  protected def onSnapshot(checkpointId: Long): Unit = if (enableLoging) {
    val measurement = snapshotMeasurement(checkpointId)
    val operatorLabel = getOperatorLabel
    MDC.put("elements", s"${measurement.elements}")
    MDC.put("latency", s"${measurement.latency}")
    MDC.put("checkpointLatency", s"${measurement.checkpointLatency}")
    MDC.put("category", getCategoryLabel)
    MDC.put("operator", operatorLabel)
    logWithMdc(s"$operatorLabel snapshotting: ${measurement.toString}", "snapshot")
    MDC.remove("elements")
    MDC.remove("latency")
    MDC.remove("checkpointLatency")
    MDC.remove("category")
    MDC.remove("operator")
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    val currentCheckpoint = context.getCheckpointId
    onSnapshot(currentCheckpoint)
  }

}
