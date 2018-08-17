package org.codefeedr.jobs

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.codefeedr.core.library.LibraryServices
import org.codefeedr.core.library.internal.kafka.sink.KafkaGenericSink

import scala.concurrent._
import scala.concurrent.duration._

object LongTupleInputJob {}
