package org.codefeedr.plugins

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.codefeedr.configuration.{ConfigurationProvider, ConfigurationProviderComponent}
import org.codefeedr.util._
import org.joda.time.DateTime

import scala.collection.JavaConverters._
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec}



/**
  * Test generator that just exposes the passed seed
  * Used for unit testing the generator source
  * @param baseSeed the element that will be exposed
  */
class SeedGenerator(baseSeed:Long) extends BaseSampleGenerator[Long](baseSeed)(DateTime.now) {
  /**
    * Implement to generate a random value
    * @return
    */
  override def generate(): Long = baseSeed
}



class GeneratorSourceSpec
  extends FlatSpec
  with BeforeAndAfterAll
  with BeforeAndAfterEach
  with MockitoSugar
  with GeneratorSourceComponent
  with ConfigurationProviderComponent {

  val configurationProvider:ConfigurationProvider = mock[ConfigurationProvider]
  private var sourceContext:SourceFunction.SourceContext[Long] = _
  private val runtimeContext:StreamingRuntimeContext = mock[StreamingRuntimeContext]
  private val generator = (i:Long) => new SeedGenerator(i)

  "GeneratorSource.run" should "call generator with a unique seed" in {
    //Arrange
    val source = new GeneratorSource[Long](generator,3,"testGenerator")
    val collector = new CallbackCollector[Long](4,() => source.cancel())

    //Act
    source.run(collector)

    //Assert
    assert(collector.getElements == List(3,6,9,12))
  }

  "GeneratorSource.SnapshotState" should "Store the offset of the number of collected elements" in {
    //Arrange
    val stateStore = new TestListState[GeneratorSourceState]
    val source = getInitializedGeneratorSource
    source.initializeState(TestFunctionInitializationContext(isRestored = false,stateStore))

    //Act
    val collector = new CallbackCollector[Long](4,() => {
      source.snapshotState(new TestSnapshotContext(1))
      source.cancel()
    })
    source.run(collector)

    //Assert
    assert(stateStore.get().asScala.toList.size == 1)
    assert(stateStore.get().asScala.toList.head.currentPosition == 4)
  }

  "GeneratorSource.RestoreState" should "Cause the source to start generating from the checkpointed offset" in {
    //Arrange
    val stateStore = new TestListState[GeneratorSourceState]
    stateStore.add(GeneratorSourceState(4))
    val source = getInitializedGeneratorSource
    source.initializeState(TestFunctionInitializationContext(isRestored = true,stateStore))

    //Act
    val collector = new CallbackCollector[Long](4,() => {
      source.snapshotState(new TestSnapshotContext(1))
      source.cancel()
    })
    source.run(collector)

    //Assert
    assert(collector.getElements == List(15,18,21,24))
    assert(stateStore.get().asScala.toList.size == 1)
    assert(stateStore.get().asScala.toList.head.currentPosition == 8)
  }


  private def getInitializedGeneratorSource = {
    val sourceGenerator = new GeneratorSource[Long](generator,3,"testGenerator")
    sourceGenerator.setRuntimeContext(runtimeContext)
    sourceGenerator
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    when(configurationProvider.getInt("generator.batch.size")).thenReturn(4)
    when(runtimeContext.isCheckpointingEnabled) thenReturn true
  }


}
