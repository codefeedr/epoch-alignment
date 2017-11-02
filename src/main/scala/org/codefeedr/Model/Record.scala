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

import org.apache.flink.api.common.typeinfo.{TypeInfo, TypeInformation}
import org.apache.flink.types.Row

/**
  * A record for which its source has been trailed
  */
trait TrailedRecord extends Record {
  def trail: RecordSourceTrail = row.getField(row.getArity() - 3).asInstanceOf[RecordSourceTrail]
}

/**
  * A codefeedr record, which is registered in the metamodel
  */
trait Record {
  def row: Row

  /**
    * UUID of the type that the record represents
    * @return
    */
  def typeUuid: String = row.getField(row.getArity() - 1).asInstanceOf[String]

  /**
    * Type of action that the record represents
    * @return
    */
  def action: ActionType.Value = row.getField(row.getArity() - 2).asInstanceOf[ActionType.Value]

  /**
    * Get a field of a specific index
    * @param index index of the field to retrieve
    * @return the value at the index
    */
  def field(index: Int): Any = row.getField(index)

  /**
    * Exposes the data in the flink table api row as iterable
    * The data does not contain metadata fields, that are retrievably by methods on the interface
    * @return
    */
  def data: Iterable[Any] = for (i <- 0 to row.getArity() - 4) yield row.getField(i)
}

case class RecordProperty[T](name: String, propertyType: TypeInformation[T], id: Boolean)
    extends Serializable

/**
  * A record with its trail
  * Make sure that the row has the actual properties belonging to the trailedrecord set on the row when calling this constructor
  * TODO: Perform checks if the row is properly typed when constructing a trailed record
  * @param row The Flink tableAPI row representing the trailedRecord
  */
case class TrailedRecordRow(row: Row) extends TrailedRecord

/**
  * A record event
  * TODO: Perform checks if the row is properly typed when constructing a record
  * Does not contain the trail, because the trail is used as key in the event
  */
case class RecordRow(row: Row) extends Record

abstract class RecordSourceTrail

case class ComposedSource(SourceId: Array[Byte], pointers: Array[RecordSourceTrail])
    extends RecordSourceTrail
    with Serializable

case class Source(SourceId: Array[Byte], Key: Array[Byte])
    extends RecordSourceTrail
    with Serializable

object Record {

  /**
    * Construct a record based on some data, typeDefinition and actionType
    * @param data data to construct record for
    * @param actionType type of the action belonging to the record
    * @param typeUuid uuid of the record
    * @return
    */
  def apply(data: Array[Any], typeUuid: String, actionType: ActionType.Value): Record = {
    val row = new Row(data.length + 3)
    for (i <- data.indices) {
      row.setField(i, data(i))
    }
    row.setField(data.length + 2, typeUuid)
    row.setField(data.length + 1, actionType)

    RecordRow(row)
  }
}

object TrailedRecord {

  /**
    * Construct a trailedRecord from a record and trail
    * @param trail the trail of the record
    * @param record the record itself
    * @return
    */
  def apply(record: Record, trail: RecordSourceTrail): TrailedRecord = {
    record.row.setField(record.row.getArity() - 3, trail)
    TrailedRecordRow(record.row)
  }

  /**
    * Constructor with row as source
    * Assumes all properties have been set on the row accordingly
    * @param row
    * @return
    */
  def apply(row: Row): TrailedRecord = TrailedRecordRow(row)
}

/**
  * Data equals audit trail
  */
object ActionType extends Enumeration {
  val Add, Update, Remove = Value
}
