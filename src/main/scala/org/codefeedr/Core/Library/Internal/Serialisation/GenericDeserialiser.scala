package org.codefeedr.Core.Library.Internal.Serialisation

import java.io.{ByteArrayInputStream, ObjectInputStream, ObjectStreamClass}

import scala.reflect.ClassTag

class GenericDeserialiser[T: ClassTag](implicit ct: ClassTag[T]) {

  @transient lazy private val loader: ClassLoader = Thread.currentThread().getContextClassLoader

  private val deserializeInternal =
  /**
    * Prevent double serialization for the cases where bytearrays are directly sent to kafka
    */
    if (classOf[Array[Byte]].isAssignableFrom(ct.getClass)) { (data: Array[Byte]) =>
      data.asInstanceOf[T]
    } else { (data: Array[Byte]) =>
    {
      val ois = new ObjectInputStream(new ByteArrayInputStream(data)) {

        //Custom class loader needed
        //See: https://issues.scala-lang.org/browse/SI-9777
        override def resolveClass(desc: ObjectStreamClass): Class[_] =
        Class.forName(desc.getName, false, loader)
      }
      val value = ois.readObject()
      ois.close()
      value.asInstanceOf[T]
    }
    }

  /**
    * Serialize the element to byte array
    * @param data the data to serialize
    * @return the data as bytearray
    */
  def Deserialize(data: Array[Byte]):T = deserializeInternal(data)
}
