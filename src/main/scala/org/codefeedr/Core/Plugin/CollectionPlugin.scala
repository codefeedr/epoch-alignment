

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
