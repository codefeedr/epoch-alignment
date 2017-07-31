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

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.Model.{PropertyType, RecordProperty, SubjectType}

import scala.reflect.runtime.{universe => ru}

/**
  * Thread safe
  * Created by Niels on 14/07/2017.
  */
object SubjectTypeFactory extends LazyLogging {
  private def newTypeIdentifier(): UUID = UUID.randomUUID()

  private def getSubjectTypeInternal(t: ru.Type, idFields: Array[String]): SubjectType = {
    val properties = t.members.filter(o => !o.isMethod)
    val name = t.typeSymbol.name.toString
    SubjectType(
      newTypeIdentifier().toString,
      name,
      getDefaultProperties(idFields.length == 0) ++ properties.map(getRecordProperty(idFields)))
  }

  private def getRecordProperty(idFields: Array[String])(symbol: ru.Symbol): RecordProperty = {
    val name = symbol.name.toString.trim
    //logger.debug(f"property type of $name: ${symbol.info.toString}")
    val propertyType = symbol.typeSignature.typeSymbol.name.toString match {
      case "scala.Int" => PropertyType.Number
      case "Int" => PropertyType.Number
      case "String" => PropertyType.String
      case _ => PropertyType.Any
    }

    RecordProperty(name, propertyType, idFields.contains(name))
  }

  /**
    * Generate the default property definitions added by our framework
    * @param isId are the auto generated fields ids
    * @return Array of the default properties
    */
  private def getDefaultProperties(isId: Boolean) =
    Array(
      RecordProperty("_Type", PropertyType.String, id = true),
      RecordProperty("_Sequence", PropertyType.Number, isId),
      RecordProperty("_Source", PropertyType.String, isId)
    )

  /**
    * Get a subject type for the query language, type tag required
    * @tparam T type of the subject
    * @return Type description of the given type
    */
  def getSubjectType[T: ru.TypeTag]: SubjectType =
    getSubjectTypeInternal(ru.typeOf[T], Array.empty[String])

  /**
    * Get subject type for the query language, type tag required
    * @param idFields Names of the fields that uniquely identify the record
    * @tparam T Type of the object
    * @return Type description of the given type
    */
  def getSubjectType[T: ru.TypeTag](idFields: Array[String]): SubjectType =
    getSubjectTypeInternal(ru.typeOf[T], idFields)

}
