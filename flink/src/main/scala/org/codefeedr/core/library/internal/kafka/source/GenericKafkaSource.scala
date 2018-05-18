package org.codefeedr.core.library.internal.kafka.source

trait GenericKafkaSource {
  def cancel(): Unit
}
