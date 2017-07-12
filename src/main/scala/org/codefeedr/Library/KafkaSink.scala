package org.codefeedr.Library

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
  * Created by Niels on 11/07/2017.
  */
class KafkaSink[TData] extends RichSinkFunction[TData] {
  override def invoke(value: TData): Unit = {}
}
