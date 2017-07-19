package org.codefeedr.Library

import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.runtime.{universe => ru}
import org.apache.flink.api.scala._

/**
  * Created by Niels on 18/07/2017.
  */
object KafkaSubjectFactory {
  def GetSink[TData: ru.TypeTag]: Future[KafkaSink[TData]] = {
    KafkaLibrary.GetType[TData]().map(o => new KafkaSink[TData](o))
  }

  def GetSource[TData: ru.TypeTag: TypeInformation]: Future[KafkaSource[TData]] = {
    KafkaLibrary.GetType[TData]().map(o => new KafkaSource[TData](o))
  }
}
