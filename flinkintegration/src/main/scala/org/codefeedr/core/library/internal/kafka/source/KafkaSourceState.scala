package org.codefeedr.core.library.internal.kafka.source

/**
  * The states a kafkasource can be in
  */
object KafkaSourceState extends Enumeration {
  val UnSynchronized, CatchingUp, Ready, Synchronized, Closing = Value
}

object KafkaSourceStateTransition extends Enumeration {
  val None, CatchUp, Aborting = Value
}
