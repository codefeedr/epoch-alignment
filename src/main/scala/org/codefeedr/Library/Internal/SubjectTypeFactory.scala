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

package org.codefeedr.Library.Internal

import java.lang.reflect.Field
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.Model.{PropertyType, RecordProperty, SubjectType}

import scala.reflect.runtime.{universe => ru}

/**
  * Created by Niels on 14/07/2017.
  */
object SubjectTypeFactory extends LazyLogging {
  private def newTypeIdentifier(): UUID = UUID.randomUUID()

  private def getSubjectTypeInternal(t: ru.Type): SubjectType = {
    val properties = t.getClass.getDeclaredFields
    val name = t.typeSymbol.name.toString
    SubjectType(newTypeIdentifier().toString, name, properties.map(getRecordProperty))
  }

  private def getRecordProperty(field: Field): RecordProperty = {
    //logger.debug(f"property type of $name: ${symbol.info.toString}")
    val propertyType = field.getType.getName match {
      case "scala.Int" => PropertyType.Number
      case "Int" => PropertyType.Number
      case "String" => PropertyType.String
      case _ => PropertyType.Any
    }

    RecordProperty(field.getName, propertyType)
  }

  /**
    * Get a subject type for the query language, type tag required
    * @tparam T type of the subject
    * @return Type description of the given type
    */
  def getSubjectType[T: ru.TypeTag]: SubjectType = getSubjectTypeInternal(ru.typeOf[T])

}
