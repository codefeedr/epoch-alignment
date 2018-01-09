/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.codefeedr.Core.Library.Internal.Serialisation

import java.io.{ByteArrayInputStream, ObjectInputStream, ObjectStreamClass}
import java.net.URLClassLoader

import scala.reflect.ClassTag

class GenericDeserialiser[T: ClassTag](implicit ct: ClassTag[T]) {

  @transient lazy private val loader: ClassLoader = getClass.getClassLoader

  private val deserializeInternal =
    /**
      * Prevent double serialization for the cases where bytearrays are directly sent to kafka
      */
    if (classOf[Array[Byte]].isAssignableFrom(ct.getClass)) { (data: Array[Byte]) =>
      data.asInstanceOf[T]
    } else { (data: Array[Byte]) =>
      {
        if (data != null) {
          val ois = new ObjectInputStream(new ByteArrayInputStream(data)) {

            //Custom class loader needed
            //See: https://issues.scala-lang.org/browse/SI-9777
            override def resolveClass(desc: ObjectStreamClass): Class[_] =
              Class.forName(desc.getName, false, loader)
          }
          val value = ois.readObject()
          ois.close()
          value.asInstanceOf[T]
        } else {
          null.asInstanceOf[T]
        }
      }
    }

  /**
    * Serialize the element to byte array
    * @param data the data to serialize
    * @return the data as bytearray
    */
  def Deserialize(data: Array[Byte]): T = deserializeInternal(data)
}

/**
  * Deserialise an object serialised by the GenericSerialiser
  */
object GenericDeserialiser {
  def apply[TData: ClassTag](data: Array[Byte]): TData = {
    new GenericDeserialiser[TData]().Deserialize(data)
  }
}
