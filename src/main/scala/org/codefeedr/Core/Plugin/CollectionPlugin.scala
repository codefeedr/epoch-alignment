

package org.codefeedr.Core.Plugin

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

/**
  * A simple collection plugin that registers a static dataset as plugin
  * @param data The data of the collection
  * @tparam TData Type of the data
  */
class CollectionPlugin[TData: ru.TypeTag: ClassTag: TypeInformation](data: Array[TData])
    extends SimplePlugin[TData] {

  /**
    * Method to implement as plugin to expose a datastream
    * Make sure this implementation is serializable!!!!
    *
    * @param env The environment to create the datastream om
    * @return The datastream itself
    */
  override def GetStream(env: StreamExecutionEnvironment): DataStream[TData] = {
    env.fromCollection[TData](data)
  }
}
