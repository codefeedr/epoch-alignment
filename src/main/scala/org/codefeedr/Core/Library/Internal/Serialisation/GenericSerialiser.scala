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
