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

import java.util.UUID

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInfo, TypeInformation}
import org.codefeedr.Model.{RecordProperty, SubjectType}
import org.apache.flink.streaming.api.scala.createTypeInformation

import scala.reflect.api
import scala.reflect.runtime.{universe => ru}

/**
  * Thread safe
  * Created by Niels on 14/07/2017.
  */
object SubjectTypeFactory extends LazyLogging {
  @transient lazy val conf: Config = ConfigFactory.load


  private def newTypeIdentifier(): UUID = UUID.randomUUID()

  private def getSubjectTypeInternal(t: ru.Type,
                                     idFields: Array[String],
                                     persistent: Boolean): SubjectType = {
    val properties = t.members.filter(o => !o.isMethod)
    val name = getSubjectName(t)
    val r = SubjectType(newTypeIdentifier().toString,
                        name,
                        persistent = persistent,
                        properties = properties.map(getRecordProperty(idFields)).toArray)
    if (r.properties.count(o => o.id) != idFields.length) {
      val msg = s"Some idfields given to getSubjectType did not exist: ${idFields
        .filter(o => !r.properties.map(o => o.name).contains(o))
        .mkString(", ")}"
      val e = new Exception(msg)
      logger.error(msg, e)
      throw e
    }
    r
  }

  private def getRecordProperty(idFields: Array[String])(symbol: ru.Symbol): RecordProperty[_] = {
    val name = symbol.name.toString.trim
    //logger.debug(f"property type of $name: ${symbol.info.toString}")

    //TODO: Is this valid code?
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val propertyType = TypeInformation.of(mirror.runtimeClass(symbol.typeSignature))

    RecordProperty(name, propertyType, idFields.contains(name))
  }

  /**
    * Use a generic type to retrieve the subjectName that the given type would produce
    * @tparam T the type to retrieve th name for
    * @return the name
    */
  def getSubjectName[T: ru.TypeTag]: String = getSubjectName(ru.typeOf[T])

  /**
    * Use the type to retrieve the subjectName that the given type woul produce
    * @param t the type to retrieve name for
    * @return the name of the subject
    */
  def getSubjectName(t: ru.Type): String = t.typeSymbol.name.toString

  /**
    * Get a subject type for the query language, type tag required
    * Creates a non-persistent subject
    * @tparam T type of the subject
    * @return Type description of the given type
    */
  def getSubjectType[T: ru.TypeTag]: SubjectType = getSubjectType[T](persistent = false)

  /**
    * Get a subject type for the query language, type tag required
    * @param persistent Should the created type be persistent or not?
    * @tparam T type of the subject
    * @return Type description of the given type
    */
  def getSubjectType[T: ru.TypeTag](persistent: Boolean): SubjectType =
    getSubjectTypeInternal(ru.typeOf[T], Array.empty[String], persistent)

  /**
    * Get subject type for the query language, type tag required
    * Creates a non-persistent subject
    * @param idFields Names of the fields that uniquely identify the record
    * @tparam T Type of the object
    * @return Type description of the given type
    */
  def getSubjectType[T: ru.TypeTag](idFields: Array[String]): SubjectType =
    getSubjectType[T](idFields, persistent = false)

  /**
    * Get subject type for the query language, type tag required
    * @param idFields Names of the fields that uniquely identify the record
    * @param persistent Should the type be persistent
    * @tparam T Type of the object
    * @return Type description of the given type
    */
  def getSubjectType[T: ru.TypeTag](idFields: Array[String], persistent: Boolean): SubjectType =
    getSubjectTypeInternal(ru.typeOf[T], idFields, persistent)

  /**
    * Create a subjectType based on a name, fields and types
    * This method is called when creating a subject from the result of a query on Flink's Table API
    * @param subjectName name of the subject to create
    * @param fields its fields
    * @param propertyTypes and its properties
    * @return
    */
  def getSubjectType(subjectName: String,
                     fields: Array[String],
                     propertyTypes: Array[TypeInformation[_]]): SubjectType = {
    val properties = fields.zipWithIndex
      .map { case (v, i) => RecordProperty(v, propertyTypes(i), id = false) }
      .map(o => o.asInstanceOf[RecordProperty[_]])
    SubjectType(newTypeIdentifier().toString, subjectName, persistent = false, properties)
  }
}
