package org.codefeedr.Library

import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.runtime.{universe => ru}
import org.apache.flink.api.scala._
import org.codefeedr.Library.Internal.KafkaController

/**
  * Created by Niels on 18/07/2017.
  */
object SubjectFactory {
  def GetSink[TData: ru.TypeTag]: Future[KafkaSink[TData]] = {
    SubjectLibrary
      .GetType[TData]()
      .flatMap(o =>
        KafkaController.GuaranteeTopic(s"${o.name}_${o.uuid}").map(_ => new KafkaSink[TData](o)))
  }

  def GetSource[TData: ru.TypeTag: TypeInformation]: Future[KafkaSource[TData]] = {
    SubjectLibrary.GetType[TData]().map(o => new KafkaSource[TData](o))
  }
}
