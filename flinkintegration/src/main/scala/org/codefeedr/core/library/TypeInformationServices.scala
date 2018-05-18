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

package org.codefeedr.core.library

import org.codefeedr.model.{ActionType, RecordSourceTrail, SubjectType}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.types.Row

import collection.JavaConverters._

object TypeInformationServices {

  @transient lazy val _additionalNames = additionalNames
  @transient lazy val _additionalTypes = additionalTypes

  /**
    * Build Flinks typeInformation using a subjectType.
    * @param subjecType
    * @return
    */
  def getRowTypeInfo(subjecType: SubjectType): TypeInformation[Row] = {
    val names = subjecType.properties.map(o => o.name)
    val types = subjecType.properties.map(o => o.propertyType)
    new RowTypeInfo(types, names)
  }

  /**
    * Builds Flinks typeInformation for a row.
    * Also contains the meta-properties that are added by Codefeedr
    * TODO: Somehow optimize these additional properties away
    * @param subjectType
    * @return
    */
  def getEnrichedRowTypeInfo(subjectType: SubjectType): TypeInformation[Row] = {
    val names = subjectType.properties.map(o => o.name) ++ _additionalNames
    val types = subjectType.properties.map(o => o.propertyType) ++ _additionalTypes
    new RowTypeInfo(types, names)
  }

  /**
    * Get the additional types added to the row with meta-informaiton for codefeedr
    * @return
    */
  def additionalNames = Array("Trail", "ActionType", "TypeUuid")

  /**
    * Get the typeDefinitions of th additional types added to the row with meta-information
    * @return
    */
  def additionalTypes =
    Array(TypeInformation.of(classOf[RecordSourceTrail]),
          TypeInformation.of(classOf[ActionType.Value]),
          TypeInformation.of(classOf[String]))
}
