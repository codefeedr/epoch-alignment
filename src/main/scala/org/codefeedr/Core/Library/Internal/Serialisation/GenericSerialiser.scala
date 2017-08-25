package org.codefeedr.Core.Library.Internal.Serialisation

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import scala.reflect.ClassTag

class GenericSerialiser[T: ClassTag](implicit ct: ClassTag[T]) {
  private val serializeInternal: (T) => Array[Byte] = {

    /**
      * Prevent deserialisation of something that already is a byte array, as those are also not serialized
      */
    if (classOf[Array[Byte]].isAssignableFrom(ct.getClass)) { (data: T) =>
      data.asInstanceOf[Array[Byte]]
    } else { (data: T) =>
      {
        val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
        val oos = new ObjectOutputStream(stream)
        oos.writeObject(data)
        oos.close()
        stream.toByteArray
      }
    }
  }

  /**
    * Serialize the element to byte array
    * @param data the data to serialize
    * @return the data as bytearray
    */
  def Serialize(data: T): Array[Byte] = serializeInternal(data)
}

/**
  * Serialise the given object to byte array
  */
object GenericSerialiser {
  def apply[TData: ClassTag](tData: TData): Array[Byte] = {
    new GenericSerialiser[TData]().Serialize(tData)
  }
}
