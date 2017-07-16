package org.codefeedr.Library

import org.codefeedr.Library.Internal.KafkaController
import scala.concurrent.{Future, Await}

/**
  * Created by Niels on 14/07/2017.
  */
object KafkaLibrary {

  def RegisterSink[TData](kafkaSink: KafkaSink[TData]): Long = { 1L }

  def unregisterSink[TData](kafkaSink: KafkaSink[TData]) = {}

}
