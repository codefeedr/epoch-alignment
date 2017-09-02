

package org.codefeedr.Core.Library.Internal

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.codefeedr.Model.SubjectType

import scala.concurrent.Future

/**
  * Created by Niels on 04/08/2017.
  */
abstract class AbstractPlugin {

  /**
    * Create a new subjecttype for the plugin
    * @return
    */
  def CreateSubjectType(): SubjectType
  def Compose(env: StreamExecutionEnvironment): Future[Unit]
}
