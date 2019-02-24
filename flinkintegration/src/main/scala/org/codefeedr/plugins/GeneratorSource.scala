package org.codefeedr.plugins

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
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

class GeneratorSourceState() {
  var currentPosition: Long = 0
}

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
                                     name: String,
                                     eventsPerMillisecond: Option[Long] = None,
                                     enableLogging: Boolean = true): SourceFunction[TSource]

  /**
    * Base class for generators
    *
    * @param generator Seeded generator for elements
    * @param seedBase  Base for the seed. Offset is multiplied by this value and passed as seed to the generator.
    *                  For best results, use a 64 bit prime number
    * @param eventsPerMillisecond optional limit for the amount of events that should be generated per millisecond
    * @tparam TSource the type to generate
    */
  class GeneratorSource[TSource](val generator: (Long, Long, Long) => BaseSampleGenerator[TSource],
                                 val seedBase: Long,
                                 val name: String,
                                 val eventsPerMillisecond: Option[Long] = None,
                                 override protected val enableLoging: Boolean = true)
      extends RichParallelSourceFunction[TSource]
      with CheckpointedFunction
      with CheckpointListener
      with MeasuredCheckpointedFunction {

    @volatile private var running = true
    @transient private lazy val startTime = System.currentTimeMillis()
    private var startOffset: Long = 0
    private var currentOffset: Long = 0
    private var lastEventTime: Long = 0
    private var lastLatency: Long = 0
    @volatile private var currentCheckpoint: Long = 0
    @volatile private var waitForCp: Option[Long] = None

    @transient private lazy val parallelIndex = getRuntimeContext.getIndexOfThisSubtask

    override def getRun: String = configurationProvider.get("run")
    override def getCurrentOffset: Long = currentOffset

    //Number of elements that is generated outside of the checkpointlock
    private lazy val generationBatchSize: Int = eventsPerMillisecond.getOrElse(1L).toInt
    //configurationProvider.getInt("generator.batch.size")
    //Current state object
    var state: ListState[GeneratorSourceState] = _

    def getLabel: String = s"$getOperatorLabel cp($currentCheckpoint)"
    override def getOperatorLabel: String = s"$getCategoryLabel[$parallelIndex]"
    override def getCategoryLabel: String = s"GeneratorSource $name"

    @transient lazy val getMdcMap = Map(
      "operator" -> getOperatorLabel,
      "parallelIndex" -> parallelIndex.toString
    )

    @transient private lazy val generationSource = 1 to generationBatchSize

    /**
      * Generates a sequence of elements of TSource, with the length of the configured generationBatchSize
      */
    private def generate(): Seq[Either[GenerationResponse, (TSource, Option[Long])]] =
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
    private def getState = {
      val r = new GeneratorSourceState()
      r.currentPosition = currentOffset
      r
    }

    override def run(ctx: SourceFunction.SourceContext[TSource]): Unit = {
      while (running) {
        //Generate source elements
        val nextElements = generate()
        //Collect them within the checkpoint lock, and update the current offset
        ctx.getCheckpointLock.synchronized {
          nextElements.foreach {
            case Right(v) =>
              currentOffset += 1
              v._2 match {
                case Some(time) =>
                  lastEventTime = time
                  onEvent(Some(time))
                  ctx.collectWithTimestamp(v._1, time)
                case None =>
                  ctx.collect(v._1)
              }
            case Left(w) =>
              w match {
                case WaitForNextCheckpoint(nextCp) => waitForCp = Some(nextCp)
              }
          }
          val newLatency = System.currentTimeMillis() - lastEventTime
          if (newLatency > lastLatency) {
            lastLatency = newLatency
          }
          ctx.emitWatermark(new Watermark(lastEventTime + 1))
        }

        limitThroughput()
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

    /**
      * Limits the eventgeneration if needed
      */
    private def limitThroughput(): Unit = {
      eventsPerMillisecond match {
        case Some(v) =>
          val elapsed = System.currentTimeMillis() - startTime
          val expectedEvents = elapsed * v
          if (expectedEvents < currentOffset - startOffset) {
            logger.trace("Throttling event generation")
            Thread.sleep(Math.max(generationBatchSize / v, 1))
          }
        case None =>
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

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      Thread.sleep(5000)
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
            startOffset = currentOffset
            logger.info(s"Initialized $getLabel with offset $currentOffset")
        }
      }
    }

    override def notifyCheckpointComplete(checkpointId: Long): Unit = {
      logger.info(s"$getLabel was notified of completed checkpoint $checkpointId")
    }

  }

}
