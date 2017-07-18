package org.codefeedr.Library

import scala.concurrent.Future
import scala.reflect.runtime.{universe => ru}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by Niels on 18/07/2017.
  */
object SinkFactory {
  def GetSink[TData: ru.TypeTag]: Future[KafkaSink[TData]] = {
    KafkaLibrary.GetType[TData]().map(o => new KafkaSink[TData](o))
  }
}
