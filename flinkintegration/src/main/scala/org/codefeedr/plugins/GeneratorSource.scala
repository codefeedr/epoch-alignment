package org.codefeedr.plugins

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{
  CheckpointListener,
  FunctionInitializationContext,
  FunctionSnapshotContext
}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.codefeedr.configuration.ConfigurationProviderComponent

import scala.collection.JavaConverters._

case class GeneratorSourceState(currentPosition: Long)

trait GeneratorSourceComponent { this: ConfigurationProviderComponent =>

  /**
    * Create a new generatorSource with the given generator and seedBase
    *
    * @param generator generator of source elements
    * @param seedBase  Base for the seed. Offset is multiplied by this value and passed as seed to the generator.
    *                  For best results, use a 64 bit prime number
    * @param name Name of the source, used for logging
    * @tparam TSource type of elements exposed by the source
    * @return
    */
  def createGeneratorSource[TSource](generator: Long => BaseSampleGenerator[TSource],
                                     seedBase: Long,
                                     name: String): SourceFunction[TSource]

  /**
    * Base class for generators
    *
    * @param generator Seeded generator for elements
    * @param seedBase  Base for the seed. Offset is multiplied by this value and passed as seed to the generator.
    *                  For best results, use a 64 bit prime number
    * @tparam TSource the type to generate
    */
  class GeneratorSource[TSource](val generator: Long => BaseSampleGenerator[TSource],
                                 val seedBase: Long,
                                 val name: String)
      extends RichSourceFunction[TSource]
      with CheckpointedFunction
      with CheckpointListener
      with LazyLogging {

    @volatile private var running = true
    @volatile private var currentOffset: Long = 0
    @volatile private var currentCheckpoint: Long = 0
    @transient private lazy val parallelIndex = getRuntimeContext.getIndexOfThisSubtask

    //Number of elements that is generated outside of the checkpointlock
    lazy private val generationBatchSize: Int =
      configurationProvider.getInt("generator.batch.size")
    //Current state object
    var state: ListState[GeneratorSourceState] = _

    private def getLabel: String = s"GeneratorSource $name[$parallelIndex] cp($currentCheckpoint)"

    @transient private lazy val generationSource = 1 to generationBatchSize

    /**
      * Generates a sequence of elements of TSource, with the length of the configured generationBatchSize
      */
    private def generate(): Seq[(TSource, Long)] =
      generationSource
        .map(o => generator(seedBase * (currentOffset + o)).generateWithEventTime())
        .toList

    /**
      * @return current state of this source
      */
    private def getState = GeneratorSourceState(currentOffset)

    override def run(ctx: SourceFunction.SourceContext[TSource]): Unit = {
      while (running) {
        //Generate source elements
        val nextElements = generate()
        //Collect them within the checkpoint lock, and update the current offset
        ctx.getCheckpointLock.synchronized {
          currentOffset += generationBatchSize
          nextElements.foreach(o => ctx.collectWithTimestamp(o._1, o._2))
        }
      }
    }

    override def cancel(): Unit = {
      running = false
    }

    override def snapshotState(context: FunctionSnapshotContext): Unit = {
      currentCheckpoint = context.getCheckpointId
      logger.debug(s"Snapshotting state of $getLabel")
      state.update(List(getState).asJava)
    }

    override def initializeState(context: FunctionInitializationContext): Unit = {
      if (!getRuntimeContext.asInstanceOf[StreamingRuntimeContext].isCheckpointingEnabled) {
        logger.warn(
          "GeneratorSource function without checkpointing enabled. The custom source is designed to work with checkpoints only.")
      } else {
        logger.info("Started GeneratorSource Function with checkpointing enabled")
      }
      val stateDescriptor = new ListStateDescriptor[GeneratorSourceState](
        "Custom aligning kafka sink state",
        TypeInformation.of(new TypeHint[GeneratorSourceState]() {})
      )
      state = context.getOperatorStateStore.getListState(stateDescriptor)

      if (context.isRestored) {
        val states = state.get().asScala.toList
        states.size match {
          case i if i > 1 =>
            throw new IllegalArgumentException(
              "Cannot restore GeneratorSource with more than one state")
          case i if i < 1 =>
            throw new IllegalArgumentException(
              "Cannot restore GeneratorSource with less than one state")
          case _ =>
            currentOffset = states.head.currentPosition
            logger.info(s"Initialized $getLabel with offset $currentOffset")
        }
      }
    }

    override def notifyCheckpointComplete(checkpointId: Long): Unit = {
      logger.info(s"$getLabel was notified of completed checkpoint $checkpointId")
    }
  }

}
