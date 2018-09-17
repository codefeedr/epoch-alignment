package org.codefeedr.plugins

import org.apache.flink.streaming.api.functions.source.RichSourceFunction

import scala.util.Random

/**
  * Base class for generators
  * @tparam TSource the type to generate
  */
abstract class GeneratorSource[TSource] extends RichSourceFunction[TSource] {

}
