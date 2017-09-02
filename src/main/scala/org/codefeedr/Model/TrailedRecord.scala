

package org.codefeedr.Model

/**
  * A record with its trail
  * @param record Record containing the actual data
  * @param trail Trail tacking the source of the record
  */
case class TrailedRecord(record: Record, trail: RecordSourceTrail) extends Serializable

abstract class RecordSourceTrail

case class ComposedSource(SourceId: Array[Byte], pointers: Array[RecordSourceTrail])
    extends RecordSourceTrail
    with Serializable

case class Source(SourceId: Array[Byte], Key: Array[Byte])
    extends RecordSourceTrail
    with Serializable
