package org.codefeedr.Model

import org.apache.flink.api.common.typeinfo.{PrimitiveArrayTypeInfo, TypeInformation, Types}

/**
  * Created by Niels on 12/07/2017.
  */
@SerialVersionUID(100L)
case class Record(id: RecordIdentifier, data: RecordData, recordType: RecordType.Value) extends Serializable

@SerialVersionUID(100L)
case class RecordData(bag: Array[Any]) extends Serializable

@SerialVersionUID(100L)
case class RecordIdentifier(sequence: Long, recordSource: Long) extends Serializable

@SerialVersionUID(100L)
case class RecordSource(id: Long, typeId: Long) extends Serializable

@SerialVersionUID(100L)
case class RecordType(id: Long, properties: Set[RecordProperty]) extends Serializable

@SerialVersionUID(100L)
case class RecordProperty(name: String, typeInfo: TypeInformation[_]) extends Serializable

object RecordType extends Enumeration {
  val Add, Update, Remove = Value
}