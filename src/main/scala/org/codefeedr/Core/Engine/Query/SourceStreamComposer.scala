package org.codefeedr.Core.Engine.Query

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.codefeedr.Core.Library.SubjectFactory
import org.codefeedr.Model.{SubjectType, TrailedRecord}

/**
  * Created by Niels on 31/07/2017.
  */
class SourceStreamComposer(subjectType: SubjectType) extends StreamComposer {
  override def Compose(env: StreamExecutionEnvironment): DataStream[TrailedRecord] = {
    env.addSource(SubjectFactory.GetSource(subjectType))
  }

  /**
    * Retrieve typeinformation of the type that is exposed by the Streamcomposer (Note that these types are not necessarily registered on kafka, as it might be an intermediate type)
    *
    * @return Typeinformation of the type exposed by the stream
    */
  override def GetExposedType(): SubjectType = subjectType
}
