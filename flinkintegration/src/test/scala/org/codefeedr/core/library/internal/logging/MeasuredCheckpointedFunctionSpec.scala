package org.codefeedr.core.library.internal.logging
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.scalatest.FlatSpec


import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar


class TestMeasuredCheckpointedFunction extends MeasuredCheckpointedFunction {
  override def getCurrentOffset: Long = 10
  override def initializeState(context: FunctionInitializationContext): Unit = {}
  override def getMdcMap: Map[String, String] = Map.empty[String,String]
  override def getOperatorLabel: String = "TestMeasuredCheckpointedFunction"
  override def getCategoryLabel: String = "TestMeasuredCheckpointedFunction"

  override def currentMillis(): Long = 1000
}

class MeasuredCheckpointedFunctionSpec extends FlatSpec with MockitoSugar {

  "snapshotMeasurement" should "return the maximum latency during the checkpoint" in {
    //Arrange
    val measurementFunction = createMeasurementFunction
    measurementFunction.onEvent(Some(100))
    measurementFunction.onEvent(Some(200))

    //Act
    val measurement = measurementFunction.snapshotMeasurement(1)

    //Assert
    assert(measurement.checkpointLatency == 900)
    assert(measurement.latency == 900)
    assert(measurement.elements == 10)
    assert(measurement.offset == 10)
  }


  "snapshotState" should "reset the counts" in {
    //Arrange
    val measurementFunction = createMeasurementFunction
    measurementFunction.onEvent(Some(100))
    measurementFunction.onEvent(Some(200))

    val contextMock = mock[FunctionSnapshotContext]
    when(contextMock.getCheckpointId) thenReturn 1

    //Act
    measurementFunction.snapshotState(contextMock)
    val measurement = measurementFunction.snapshotMeasurement(2)


    //Assert
    assert(measurement.latency == 0)
    assert(measurement.checkpointLatency == 0)
  }

  private def createMeasurementFunction:TestMeasuredCheckpointedFunction = {
    new TestMeasuredCheckpointedFunction()
  }

}

