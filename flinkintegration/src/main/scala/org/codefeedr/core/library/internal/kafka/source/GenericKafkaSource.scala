package org.codefeedr.core.library.internal.kafka.source
import org.codefeedr.core.library.metastore.sourcecommand.SourceCommand

trait GenericKafkaSource {
  def cancel(): Unit
  def apply(command: SourceCommand): Unit
}
