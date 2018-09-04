package org.codefeedr.core.plugin

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeutils.base.IntSerializer
import org.apache.flink.core.memory.DataInputView
import org.apache.flink.core.memory.DataInputViewStreamWrapper
import org.apache.flink.core.memory.DataOutputViewStreamWrapper
import org.apache.flink.runtime.state.{
  CheckpointListener,
  FunctionInitializationContext,
  FunctionSnapshotContext
}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.util.Preconditions
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.util.Try

/**
  * This file was copied from https://github.com/apache/flink/blob/master/flink-streaming-java/src/test/java/org/apache/flink/streaming/api/functions/FromElementsFunctionTest.java
  * But (automatically) transformed to scala and modified, so it waits for the final checkpoint to complete before finishing the run function
  */
/**
  * A stream source function that returns a sequence of elements.
  *
  * <p>Upon construction, this source function serializes the elements using Flink's type information.
  * That way, any object transport using Java serialization will not be affected by the serializability
  * of the elements.</p>
  *
  * <p><b>NOTE:</b> This source has a parallelism of 1.
  *
  */
@SerialVersionUID(1L)
object WaitingFromElementsFunction {

  /**
    * Verifies that all elements in the collection are non-null, and are of the given class, or
    * a subclass thereof.
    *
    * @param elements The collection to check.
    * @param viewedAs The class to which the elements must be assignable to.
    */
  // ------------------------------------------------------------------------
  //  Checkpointing
  // ------------------------------------------------------------------------
  // ------------------------------------------------------------------------
  //  Utilities
  // ------------------------------------------------------------------------
  def checkCollection[OUT](elements: util.Collection[OUT], viewedAs: Class[OUT]): Unit = {
    import scala.collection.JavaConversions._
    for (elem <- elements) {
      if (elem == null)
        throw new IllegalArgumentException("The collection contains a null element")
      if (!viewedAs.isAssignableFrom(elem.getClass))
        throw new IllegalArgumentException(
          "The elements in the collection are not all subclasses of " + viewedAs.getCanonicalName)
    }
  }
}

@SerialVersionUID(1L)
class WaitingFromElementsFunction[T](
                                     /** The (de)serializer to be used for the data elements. */
                                     val serializer: TypeSerializer[T],
                                     val elements: Iterable[T])
    extends RichSourceFunction[T]
    with CheckpointedFunction
    with LazyLogging
    with CheckpointListener {

  def init(): (Array[Byte], Int) = {
    val baos = new ByteArrayOutputStream()
    val wrapper = new DataOutputViewStreamWrapper(baos)
    var count = 0
    try {
      import scala.collection.JavaConversions._
      for (element <- elements) {
        serializer.serialize(element, wrapper)
        count += 1
      }
    } catch {
      case e: Exception =>
        throw new IOException("Serializing the source elements failed: " + e.getMessage, e)
    }
    (baos.toByteArray, count)
  }

  /** The actual data elements, in serialized form. */
  @transient private lazy val (elementsSerialized: Array[Byte], numElements: Int) = init()

  /** The number of serialized elements. */
  //private var numElements = 0
  /** The number of elements emitted already. */
  private var numElementsEmitted = 0

  /** The number of elements to skip initially. */
  private var numElementsToSkip = 0

  /** Flag to make the source cancelable. */
  private var isRunning = true

  private var checkpointedState: ListState[Integer] = _

  /** boolean indicating the run thread it can safely exit */
  @volatile private var finalCheckpointReached = false

  /** Indicates at which checkpoint the job should terminate */
  private var finalCheckpointId: Option[Long] = None

  @throws[Exception]
  override def initializeState(context: FunctionInitializationContext): Unit = {

    if (!getRuntimeContext().asInstanceOf[StreamingRuntimeContext].isCheckpointingEnabled) {
      logger.warn(
        "Waiting From Elements function without checkpointing enabled. The custom source is designed to work with checkpoints only.")
      finalCheckpointReached = true
      // throw new Error(
      // "Started a custom source without checkpointing enabled. The custom source is designed to work with checkpoints only.")

    } else {
      logger.info("Started Waiting From Elements Function with checkpointing enabled")
      finalCheckpointReached = false
    }

    Preconditions.checkState(checkpointedState == null,
                             "The " + getClass.getSimpleName + " has already been initialized.",
                             null)
    checkpointedState = context.getOperatorStateStore.getListState(
      new ListStateDescriptor[Integer]("from-elements-state", IntSerializer.INSTANCE))
    if (context.isRestored) {
      val retrievedStates = new util.ArrayList[Integer]
      import scala.collection.JavaConversions._
      for (entry <- checkpointedState.get) {
        retrievedStates.add(entry)
      }
      // given that the parallelism of the function is 1, we can only have 1 state
      Preconditions.checkArgument(retrievedStates.size == 1,
                                  "WaitingFromElementsFunction retrieved invalid state.",
                                  null)
      numElementsToSkip = retrievedStates.get(0)
    }
  }

  @throws[Exception]
  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
    val bais = new ByteArrayInputStream(elementsSerialized)
    val input = new DataInputViewStreamWrapper(bais)
    // if we are restored from a checkpoint and need to skip elements, skip them now.
    var toSkip = numElementsToSkip
    if (toSkip > 0) {
      try while ({
        toSkip > 0
      }) {
        serializer.deserialize(input)
        toSkip -= 1
      } catch {
        case e: Exception =>
          throw new IOException(
            "Failed to deserialize an element from the source. " + "If you are using user-defined serialization (Value and Writable types), check the " + "serialization functions.\nSerializer is " + serializer)
      }
      numElementsEmitted = numElementsToSkip
    }
    val lock = ctx.getCheckpointLock
    while ({
      isRunning && numElementsEmitted < numElements
    }) {
      val next =
        try serializer.deserialize(input)
        catch {
          case e: Exception =>
            throw new IOException(
              "Failed to deserialize an element from the source. " + "If you are using user-defined serialization (Value and Writable types), check the " + "serialization functions.\nSerializer is " + serializer)
        }
      lock.synchronized {
        ctx.collect(next)
        numElementsEmitted += 1
      }

    }

    if (!finalCheckpointReached) {
      logger.info(
        s"From elements function is waiting for final checkpoint $finalCheckpointId to be reached")
    }

    while (!finalCheckpointReached) {
      delay(Duration(1, MILLISECONDS).fromNow)
    }
    logger.info(
      s"From elements function reached final checkpoint $finalCheckpointId and is terminating")
  }

  def delay(dur: Deadline) = {
    Try(Await.ready(Promise().future, dur.timeLeft))
  }

  override def cancel(): Unit = {
    isRunning = false
  }

  /**
    * Gets the number of elements produced in total by this function.
    *
    * @return The number of elements produced in total.
    */
  def getNumElements: Int = numElements

  /**
    * Gets the number of elements emitted so far.
    *
    * @return The number of elements emitted so far.
    */
  def getNumElementsEmitted: Int = numElementsEmitted

  @throws[Exception]
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    Preconditions.checkState(
      checkpointedState != null,
      "The " + getClass.getSimpleName + " has not been properly initialized.",
      null)
    checkpointedState.clear()
    checkpointedState.add(numElementsEmitted)
    if (finalCheckpointId.isEmpty) {
      if(numElementsEmitted == numElements)
      {
        logger.info(
          s"From elements function setting final checkpoint id to ${context.getCheckpointId + 1}")
        //Set final checkpoint to 1 higher, because if we close directly the sink might actually be closed before the commit() gets called.
        finalCheckpointId = Some(context.getCheckpointId + 1)
      }
    } else {
      logger.debug(s"From elements function is waiting for completion of checkpoint ${context.getCheckpointId}")
    }
  }

  override def notifyCheckpointComplete(checkpointId: Long): Unit = {
    if (finalCheckpointId.nonEmpty && checkpointId == finalCheckpointId.get) {
      logger.info(s"From Elements Function reached final checkpoint.")
      finalCheckpointReached = true
    } else {
      logger.debug(s"From elements function got completion notification for checkpoint $checkpointId")
    }

  }
}
