
package org.codefeedr.Core.Engine.Query

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.codefeedr.Model.{SubjectType, TrailedRecord}

/**
  * Created by Niels on 31/07/2017.
  */
abstract class StreamComposer {

  /**
    * Compose the datastream on the given environment
    * @param env Environment to compose the stream on
    * @return Datastream result of the composiiton
    */
  def Compose(env: StreamExecutionEnvironment): DataStream[TrailedRecord]

  /**
    * Retrieve typeinformation of the type that is exposed by the Streamcomposer (Note that these types are not necessarily registered on kafka, as it might be an intermediate type)
    * @return Typeinformation of the type exposed by the stream
    */
  def GetExposedType(): SubjectType
}
