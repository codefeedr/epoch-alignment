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

package org.codefeedr.Model

case class RecordProperty(name: String, propertyType: PropertyType.Value, id: Boolean)
    extends Serializable

/**
  * A record event
  * Does not contain the trail, because the trail is used as key in the event
  * @param data The raw data array of the record
  * @param typeUuid Uuid of the type of the record
  * @param action Type of event
  */
case class Record(data: Array[Any], typeUuid: String, action: ActionType.Value)
    extends Serializable

/**
  * Data equals audit trail
  */
object ActionType extends Enumeration {
  val Add, Update, Remove = Value
}

/**
  * Property types that are recognized and supported by the query language
  * Any is used for types that are unrecognized
  */
object PropertyType extends Enumeration {
  val Number, String, Any = Value
}
