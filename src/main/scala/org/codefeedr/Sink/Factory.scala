package org.codefeedr.Sink

import org.codefeedr.Library.KafkaLibrary

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.runtime.{universe => ru}

/**
  * Created by Niels on 18/07/2017.
  */
object Factory {
  def GetSink[TData: ru.TypeTag]: Future[KafkaSink[TData]] = {
    KafkaLibrary.GetType[TData]().map(o => new KafkaSink[TData](o))
  }
}
