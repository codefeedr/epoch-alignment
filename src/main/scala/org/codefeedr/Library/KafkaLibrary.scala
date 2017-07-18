package org.codefeedr.Library

/**
  * Created by Niels on 14/07/2017.
  */
object KafkaLibrary {
  def registerSink[TData](kafkaSink: KafkaSink[TData]): Long = { 1L }

  def unRegisterSink[TData](kafkaSink: KafkaSink[TData]): Unit = {}

}
