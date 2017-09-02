

package org.codefeedr.Core

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util.UUID

import org.apache.commons.lang3.Conversion.uuidToByteArray

/**
  * Created by Niels on 02/08/2017.
  */
object Util {
  def UuidToByteArray(uuid: UUID): Array[Byte] = {
    uuidToByteArray(uuid, new Array[Byte](16), 0, 16)
  }

  def serialize(array: Array[Any]): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(array)
    oos.close()
    stream.toByteArray
  }
}
