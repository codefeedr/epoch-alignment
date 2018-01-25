package org.codefeedr.Core.Output

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

class MongoSink extends RichSinkFunction[String] {

  override def invoke(value: String): Unit = {
    //TODO write to mongo client
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    //TODO open Mongo Connection
  }
}
