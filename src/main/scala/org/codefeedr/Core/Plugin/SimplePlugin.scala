

package org.codefeedr.Core.Plugin

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.codefeedr.Core.Library.Internal.{AbstractPlugin, SubjectTypeFactory}
import org.codefeedr.Core.Library.SubjectFactory
import org.codefeedr.Model.SubjectType

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

/**
  * Implement this class to expose a simple plugin
  * Created by Niels on 04/08/2017.
  */
abstract class SimplePlugin[TData: ru.TypeTag: ClassTag] extends AbstractPlugin {

  /**
    * Method to implement as plugin to expose a datastream
    * Make sure this implementation is serializable!!!!
    * @param env The environment to create the datastream om
    * @return The datastream itself
    */
  def GetStream(env: StreamExecutionEnvironment): DataStream[TData]

  override def CreateSubjectType(): SubjectType = {
    SubjectTypeFactory.getSubjectType[TData]
  }

  /**
    * Composes the source on the given environment
    * Registers all meta-information
    * @param env the environment where the source should be composed on
    * @return
    */
  override def Compose(env: StreamExecutionEnvironment): Future[Unit] = async {
    val sink = await(SubjectFactory.GetSink[TData])
    val stream = GetStream(env)
    stream.addSink(sink)
  }
}
