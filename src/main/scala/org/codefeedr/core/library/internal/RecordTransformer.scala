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

package org.codefeedr.core.library.internal

import org.codefeedr.model._

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

/**
  * Created by Niels on 23/07/2017.
  * This class can transform objects of any type into records used in the query engine
  * This class is serializable and can be distributed over the kafka environment
  * The constructor assumes that this class will only be constructed after the subjectType has actually been registered in the library
  */
class RecordTransformer[TData: ru.TypeTag: ClassTag](subjectType: SubjectType) {
  private def ct = implicitly[reflect.ClassTag[TData]]

  /**
    * Build array of accessors for a field only once, using the type information distributed by kafka
    * Make sure to crash whenever there is a mismatch between distributed typeinformation and actual object type
    */
  private val accessors = {
    subjectType.properties
      .map(o => ct.runtimeClass.getDeclaredField(o.name))
      .map(o => {
        o.setAccessible(true)
        (obj: TData) =>
          o.get(obj)
      })
  }

  /**
    * Setters, this can be used in the future for non-case class objects
    */
  /*
  private val setters = {
    subjectType.properties
      .drop(defaultPropertySize)
      .map(o => ct.runtimeClass.getDeclaredField(o.name))
      .map(o => {
        o.setAccessible(true)
        (obj: TData, value: Any) =>
          o.set(obj, value)
      })
  }*/

  private val constructor = ct.runtimeClass.getConstructors()(0)

  /**
    * Bag a generic object into a record used in the query evaluation
    * Builds a new source reference
    * @param data The object to bag
    * @param action Type of the action (Add, Update, Delete)
    * @return The record that can be pushed into the query engine
    */
  def bag(data: TData, action: ActionType.Value): Record = {
    Record(accessors.map(o => o(data)).asInstanceOf[Array[Any]], subjectType.uuid, action)
  }

  /**
    * Unbags a record into a generic type, using a by reflection created constructor
    * @param record the record to unbag
    * @return hopefully the constructed type
    */
  def unbag(record: Record): TData = {
    val args = record.data.map(o => o.asInstanceOf[AnyRef]).toArray.reverse
    val instance = constructor.newInstance(args: _*).asInstanceOf[TData]
    instance
  }
}
