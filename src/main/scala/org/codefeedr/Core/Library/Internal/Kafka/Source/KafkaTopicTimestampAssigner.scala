package org.codefeedr.Core.Library.Internal.Kafka.Source

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * This class provides the timestamp assigner for kafka topics in the codefeedr ecosystem
  *
  * Current implementation is experimental
  *
  * @tparam T
  */
class KafkaTopicTimestampAssigner[T] extends AssignerWithPunctuatedWatermarks[T]{
  override def checkAndGetNextWatermark(lastElement: T, extractedTimestamp: Long): Watermark = {
    new Watermark(extractedTimestamp-5)
  }

  override def extractTimestamp(element: T, previousElementTimestamp: Long): Long = ???
}
