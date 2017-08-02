package org.codefeedr

import java.util.UUID

import org.apache.commons.lang3.Conversion.uuidToByteArray

/**
  * Created by Niels on 02/08/2017.
  */
object Util {
  def UuidToByteArray(uuid: UUID): Array[Byte] = {
    uuidToByteArray(uuid, new Array[Byte](16), 0, 16)
  }
}
