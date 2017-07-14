package org.codefeedr.Library.Internal

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

/**
  * Created by Niels on 14/07/2017.
  */
class Serializer[T] {
  def serialise(value: Any): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close
    stream.toByteArray
  }
}
