package org.codefeedr.core.library.internal

import org.apache.flink.runtime.state.FunctionInitializationContext
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.codefeedr.core.library.internal.logging.MeasuredCheckpointedFunction
import org.codefeedr.util.EventTime
import org.codefeedr.util.EventTime._

/**
  * A Flink sinkfunction that logs latency and throughput statistics
  * @tparam TData
  */
class LoggingSinkFunction[TData: EventTime](val name: String, run: String)
    extends RichSinkFunction[TData]
    with MeasuredCheckpointedFunction {
  @transient private var gatheredEvents: Long = 0

  @transient private lazy val parallelIndex = getRuntimeContext.getIndexOfThisSubtask
  @transient lazy val getMdcMap = Map(
    "operator" -> getOperatorLabel,
    "parallelIndex" -> parallelIndex.toString
  )

  override def getOperatorLabel: String = s"$getCategoryLabel[$parallelIndex]"
  override def getCategoryLabel: String = s"LoggingSink $name"
  override def getRun: String = run

  override def invoke(value: TData): Unit = {
    gatheredEvents += 1
    onEvent(value.getEventTime)
  }

  override def getCurrentOffset: Long = gatheredEvents

  override def initializeState(context: FunctionInitializationContext): Unit = {}
}
