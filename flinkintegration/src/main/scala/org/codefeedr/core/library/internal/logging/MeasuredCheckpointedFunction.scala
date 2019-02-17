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
                                 checkpointLatency: Long,
                                 run: String)

/**
  * Checkpointed function that measures
  */
trait MeasuredCheckpointedFunction
    extends LazyMdcLogging
    with CheckpointedFunction
    with LabeledOperator {

  def getCurrentOffset: Long
  def getRun: String
  private var maxLatency: Long = 0L
  private var latestEvent: Long = 0L
  private var shouldLog: Boolean = false

  private var lastOffset: Long = 0L

  private def getCurrentLatency: Long = maxLatency

  protected val enableLoging: Boolean = true

  /**
    * Creates a snapshot of the current state with the passed checkpointId
    * Not a pure function, after calling ,the last offset is set to the current offset
    */
  def snapshotMeasurement(checkPointId: Long): CheckpointMeasurement = {
    val currentOffset = getCurrentOffset
    val measurement = CheckpointMeasurement(
      checkPointId,
      currentOffset,
      currentOffset - lastOffset,
      getCurrentLatency,
      if (latestEvent != 0) { currentMillis() - latestEvent } else { 0 },
      getRun)
    lastOffset = currentOffset
    measurement
  }

  protected def currentMillis(): Long = System.currentTimeMillis()

  /**
    * Method that should be invoked whenever an eventTime is passed
    * @param optEventTime
    */
  def onEvent(optEventTime: Option[Long]): Unit = optEventTime match {
    case Some(eventTime) =>
      shouldLog = true
      val latency = currentMillis() - eventTime
      if (latency > maxLatency) {
        maxLatency = latency
        latestEvent = eventTime
      }
    case None =>
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
    MDC.put("run", getRun)
    logWithMdc(s"$operatorLabel snapshotting: ${measurement.toString}", "snapshot")
    MDC.remove("elements")
    MDC.remove("latency")
    MDC.remove("checkpointLatency")
    MDC.remove("category")
    MDC.remove("operator")
    MDC.remove("run")
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    val currentCheckpoint = context.getCheckpointId
    onSnapshot(currentCheckpoint)
    maxLatency = 0
    latestEvent = 0
    shouldLog = false
  }
}
