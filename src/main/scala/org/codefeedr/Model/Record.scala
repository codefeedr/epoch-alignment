package org.codefeedr.Model

import java.util.UUID

import org.apache.flink.api.common.typeinfo.{PrimitiveArrayTypeInfo, TypeInformation, Types}

/**
  * Created by Niels on 12/07/2017.
  */
@SerialVersionUID(100L)
case class Record(data: Any, action: ActionType.Value) extends Serializable

@SerialVersionUID(100L)
case class RecordIdentifier(sequence: Long, sinkSource: String) extends Serializable

@SerialVersionUID(100L)
case class RecordSource(sinkUuid: String, typeUuid: String) extends Serializable

@SerialVersionUID(100L)
case class SubjectType(uuid: String, name: String, properties: Set[RecordProperty])
    extends Serializable

@SerialVersionUID(100L)
case class RecordProperty(name: String, propertyType: PropertyType.Value) extends Serializable

case class SubjectTypeEvent(subjectType: SubjectType, actionType: ActionType.Value)

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
