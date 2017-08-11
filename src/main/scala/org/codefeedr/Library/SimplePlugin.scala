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

package org.codefeedr.Library

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.codefeedr.Library.Internal.{AbstractPlugin, SubjectTypeFactory}
import org.codefeedr.Model.SubjectType

import scala.async.Async.{async, await}
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
import scala.concurrent.ExecutionContext.Implicits.global

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
    val withSink = stream.addSink(sink)
  }
}
