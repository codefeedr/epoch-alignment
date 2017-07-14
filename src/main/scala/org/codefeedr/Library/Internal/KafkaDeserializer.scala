package org.codefeedr.Library.Internal

import java.io.{ByteArrayInputStream, ObjectInputStream}

/**
  * Created by Niels on 14/07/2017.
  */
object Deserializer {
  def deserialise(bytes: Array[Byte]): Any = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject
    ois.close
    value
  }
}
