package org.codefeedr.core.library.metastore.sourcecommand

object KafkaSourceCommand extends Enumeration {
  val catchUp, synchronize: KafkaSourceCommand.Value = Value
}

case class SourceCommand(command: KafkaSourceCommand.Value, context: Option[String])
