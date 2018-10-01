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
import org.apache.flink.streaming.api.functions.source.{
  RichParallelSourceFunction,
  RichSourceFunction,
  SourceFunction
}
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.apache.flink.streaming.api.watermark.Watermark
import org.codefeedr.configuration.ConfigurationProviderComponent
import org.codefeedr.core.library.internal.logging.MeasuredCheckpointedFunction

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
  def createGeneratorSource[TSource](generator: (Long, Long, Long) => BaseSampleGenerator[TSource],
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
  class GeneratorSource[TSource](val generator: (Long, Long, Long) => BaseSampleGenerator[TSource],
                                 val seedBase: Long,
                                 val name: String)
      extends RichParallelSourceFunction[TSource]
      with CheckpointedFunction
      with CheckpointListener
      with MeasuredCheckpointedFunction {

    @volatile private var running = true
    private var currentOffset: Long = 0
    private var lastEventTime: Long = 0
    private var lastLatency: Long = 0
    @volatile private var currentCheckpoint: Long = 0
    @volatile private var waitForCp: Option[Long] = None

    @transient private lazy val parallelIndex = getRuntimeContext.getIndexOfThisSubtask

    override def getCurrentOffset: Long = currentOffset
    override def getLastEventTime: Long = lastEventTime
    override def getLatency: Long = lastLatency

    //Number of elements that is generated outside of the checkpointlock
    lazy private val generationBatchSize: Int =
      configurationProvider.getInt("generator.batch.size")
    //Current state object
    var state: ListState[GeneratorSourceState] = _

    def getLabel: String = s"$getOperatorLabel cp($currentCheckpoint)"
    override def getOperatorLabel: String = s"$getCategoryLabel[$parallelIndex]"
    override def getCategoryLabel: String = s"GeneratorSource $name"

    @transient lazy val getMdcMap = Map(
      "operator" -> name,
      "parallelIndex" -> parallelIndex.toString
    )

    @transient private lazy val generationSource = 1 to generationBatchSize

    /**
      * Generates a sequence of elements of TSource, with the length of the configured generationBatchSize
      */
    private def generate(): Seq[Either[GenerationResponse, (TSource, Long)]] =
      generationSource
        .map(
          o =>
            generator(seedBase * (parallelIndex + 1) * (currentOffset + o),
                      currentCheckpoint,
                      currentOffset + o)
              .generateWithEventTime())
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
          nextElements.foreach {
            case Right(v) => {
              logger.debug(v._1.toString)
              lastEventTime = v._2
              currentOffset += 1
              ctx.collectWithTimestamp(v._1, v._2)
            }
            case Left(w) =>
              w match {
                case WaitForNextCheckpoint(nextCp) => waitForCp = Some(nextCp)
              }
          }
          lastLatency = System.currentTimeMillis() - lastEventTime
          ctx.emitWatermark(new Watermark(lastEventTime))
        }

        //Check if run should wait for the next checkpoint
        while (waitForCp.nonEmpty) {
          if (currentCheckpoint >= waitForCp.get) {
            waitForCp = None
          } else {
            Thread.sleep(1)
          }
        }

      }
    }

    override def snapshotState(context: FunctionSnapshotContext): Unit = {
      currentCheckpoint = context.getCheckpointId
      super[MeasuredCheckpointedFunction].snapshotState(context)
      state.update(List(getState).asJava)
    }

    override def cancel(): Unit = {
      running = false
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
