/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.codefeedr.Engine.Query
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.codefeedr.Library.{SubjectFactory, SubjectLibrary}
import org.codefeedr.Model.{SubjectType, TrailedRecord}

import scala.concurrent.Future
import org.apache.flink.api.scala._

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
