package org.codefeedr.core.library.internal.kafka.source

/**
  * The states a kafkasource can be in
  */
object KafkaSourceState extends Enumeration {
  val UnSynchronized, Synchronizing, Synchronized,Closing = Value
}
