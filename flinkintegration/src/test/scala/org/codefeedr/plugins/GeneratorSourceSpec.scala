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



object SeedGenerator {
  var currentGenerated:Int = 0
}

/**
  * Test generator that just exposes the passed seed
  * Used for unit testing the generator source
  * @param baseSeed the element that will be exposed
  */
class SeedGenerator(baseSeed:Long,checkpoint:Long,offset:Long, val maxCount:Int = 4, val waitForCp:Long = 1) extends BaseSampleGenerator[Long](baseSeed,checkpoint,offset) {



  override val staticEventTime: Option[DateTime] = None
  /**
    * Implement to generate a random value
    * @return
    */
  override def generate(): Either[GenerationResponse,Long] = {
    if(SeedGenerator.currentGenerated < maxCount || checkpoint >= waitForCp) {
      SeedGenerator.currentGenerated += 1
      Right(baseSeed)
    } else {
      Left(WaitForNextCheckpoint(waitForCp))
    }

  }
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
  private val generator = (i:Long,c:Long,o:Long) => new SeedGenerator(i,c,o)

  "GeneratorSource.run" should "call generator with a unique seed" in {
    //Arrange
    val source = getInitializedGeneratorSource
    val collector = new CallbackCollector[Long](4,() => source.cancel())

    //Act
    source.run(collector)

    //Assert
    assert(collector.getElements == List(3,6,9,12))
  }

  it should "wait for the next checkpoint when the generator returns GenerationResponse WaitForNextCheckpoint" in {
    //Arrange
    val stateStore = new TestListState[GeneratorSourceState]
    val source = getInitializedGeneratorSource
    source.initializeState(TestFunctionInitializationContext(isRestored = false,stateStore))

    val collector = new CallbackCollector[Long](8,() => source.cancel())

    //Act
    val thread = new Thread {
      override def run: Unit = {
        source.run(collector)
      }
    }
    thread.start()
    thread.join(100)
    val r1 = collector.getElements
    source.snapshotState(new TestSnapshotContext(1))
    thread.join(100)
    val r2 = collector.getElements

    assert(r1 == List(3,6,9,12))
    assert(r2 == List(3,6,9,12,15,18,21,24))
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

  "getCategoryLabel" should "return a string with the operator name" in {
    //Arrange
    val source = getInitializedGeneratorSource

    //act
    val label = source.getCategoryLabel

    //Assert
    assert(label == "GeneratorSource testGenerator")
  }

  "getOperatorLabel" should "return a string with the name and its parallel index" in {
    //Arrange
    val source = getInitializedGeneratorSource

    //act
    val label = source.getOperatorLabel

    //Assert
    assert(label == "GeneratorSource testGenerator[0]")
  }


  private def getInitializedGeneratorSource = {
    val sourceGenerator = new GeneratorSource[Long](generator,3,"testGenerator")
    sourceGenerator.setRuntimeContext(runtimeContext)
    sourceGenerator
  }

  override def afterEach(): Unit = {
    SeedGenerator.currentGenerated = 0
    super.afterEach()
  }

  override def beforeEach(): Unit = {
    SeedGenerator.currentGenerated = 0
    super.beforeEach()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    when(configurationProvider.getInt("generator.batch.size")).thenReturn(4)
    when(runtimeContext.isCheckpointingEnabled) thenReturn true
  }

  /**
    * Create a new generatorSource with the given generator and seedBase
    *
    * @param generator generator of source elements
    * @param seedBase  Base for the seed. Offset is multiplied by this value and passed as seed to the generator.
    *                  For best results, use a 64 bit prime number
    * @param name      Name of the source, used for logging
    * @tparam TSource type of elements exposed by the source
    * @return
    */
  override def createGeneratorSource[TSource](generator: (Long,Long,Long) => BaseSampleGenerator[TSource], seedBase: Long, name: String): SourceFunction[TSource] = ???
}
