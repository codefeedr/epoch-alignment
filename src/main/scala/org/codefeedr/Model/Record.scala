

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
