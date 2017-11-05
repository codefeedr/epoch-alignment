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

package org.codefeedr.Core.Library.Internal

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.Model.{Record, SubjectType}

import scala.reflect.ClassTag

/**
  * Created by Niels on 28/07/2017.
  * Utility class for some subjectType
  */
class RecordUtils(subjectType: SubjectType) extends LazyLogging {

  /**
    * Get a property of the given name and type on a record
    * Not optimized, but easy to use
    * @param property The name of the property
    * @param record The record to retrieve the property from
    * @tparam TValue Expected type of the property
    * @return The value
    * @throws Exception when the property was not found, of a different type or the record type has not yet been registered in the library
    */
  def getValueT[TValue: ClassTag](property: String)(implicit record: Record): TValue =
    getValue(property)(record).asInstanceOf[TValue]

  /**
    * Get a property of the given name and type on a record
    * Not optimized, but easy to use
    * @param property The name of the property
    * @param record The record to retrieve the property from
    * @return The value
    * @throws Exception when the property was not found or the record type has not yet been registered in the library
    */
  def getValue(property: String)(implicit record: Record): Any = {
    val propertyIndex = subjectType.properties
      .indexWhere(o => o.name == property)
    if (propertyIndex == -1) {
      val msg = s"Property $propertyIndex was not found on type ${subjectType.name}"
      val error = new Exception(msg)
      logger.error(error.getMessage, error)
      throw error
    }
    record.field(propertyIndex)
  }

  /**
    * Retrieve the respective indices of the properties on
    * @param properties Properties to find on the subject
    * @return array of indices, sorted in the same order as the given properties array
    */
  def getIndices(properties: Array[String]): Array[Int] = {
    val r = properties.map(prop => subjectType.properties.indexWhere(o => o.name == prop))
    if (r.contains(-1)) {
      val error = new Exception(
        s"Some properties given to getSubjectType did not exist: ${properties
          .filter(o => !subjectType.properties.exists(p => p.name == o))
          .mkString(", ")}")
      logger.error(error.getMessage, error)
      throw error
    }
    r
  }
}
