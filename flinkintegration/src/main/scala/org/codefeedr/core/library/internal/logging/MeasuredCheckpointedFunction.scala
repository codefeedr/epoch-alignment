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
                                 avgLatency: Long,
                                 checkpointLatency: Long,
                                 run: String)

/**
  * Checkpointed function that measures
  */
trait MeasuredCheckpointedFunction
    extends LazyMdcLogging
    with CheckpointedFunction
    with LabeledOperator {

  private var maxLatency: Long = 0L
  private[MeasuredCheckpointedFunction] var latencySum: Long = 0L
  private[MeasuredCheckpointedFunction] var eventCount: Long = 0L
  private var latestEvent: Long = Int.MaxValue
  private var shouldLog: Boolean = false
  private var lastOffset: Long = 0L

  private def getCurrentLatency: Long = maxLatency

  protected val enableLoging: Boolean = true

  def getCurrentOffset: Long
  def getRun: String

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
      if (latencySum == 0) { 0 } else { latencySum / eventCount },
      if (latestEvent != Int.MaxValue) { currentMillis() - latestEvent } else { 0 },
      getRun
    )
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
      latencySum += latency
      eventCount += 1
      if (latency > maxLatency) {
        maxLatency = latency

      }
      if (latestEvent > eventTime) {
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
    MDC.put("max_latency", s"${measurement.latency}")
    MDC.put("average_latency", s"${measurement.avgLatency}")
    MDC.put("checkpointLatency", s"${measurement.checkpointLatency}")
    MDC.put("category", getCategoryLabel)
    MDC.put("operator", operatorLabel)
    MDC.put("run", getRun)
    logWithMdc(s"$operatorLabel snapshotting: ${measurement.toString}", "snapshot")
    MDC.remove("elements")
    MDC.remove("max_latency")
    MDC.remove("average_latency")
    MDC.remove("checkpointLatency")
    MDC.remove("category")
    MDC.remove("operator")
    MDC.remove("run")
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    val currentCheckpoint = context.getCheckpointId
    onSnapshot(currentCheckpoint)
    maxLatency = 0
    eventCount = 0
    latencySum = 0
    latestEvent = Int.MaxValue
    shouldLog = false
  }
}
