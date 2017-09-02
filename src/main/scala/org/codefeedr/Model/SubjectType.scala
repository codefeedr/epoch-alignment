

package org.codefeedr.Model

case class SubjectType(uuid: String,
                       name: String,
                       persistent: Boolean,
                       properties: Array[RecordProperty])
    extends Serializable
