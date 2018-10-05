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

package org.codefeedr.core.library.internal.serialisation

import java.io.IOException

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.core.memory.DataOutputSerializer

import scala.reflect.ClassTag

class GenericSerialiser[T: ClassTag]()(implicit val ec:ExecutionConfig) {
  @transient private lazy val outputSerializer = new DataOutputSerializer(16)
  @transient private lazy val ct = implicitly[ClassTag[T]]
  @transient implicit lazy val ti:TypeInformation[T] = TypeInformation.of(ct.runtimeClass.asInstanceOf[Class[T]])
  @transient private lazy val serializer: TypeSerializer[T] = ti.createSerializer(ec)

  private val serializeInternal: (T) => Array[Byte] = {
    /**
      * Prevent deserialisation of something that already is a byte array, as those are also not serialized
      */
      if (classOf[Array[Byte]].isAssignableFrom(ct.getClass)) { data: T =>
        data.asInstanceOf[Array[Byte]]
      }  else {
      data:T => serializeValue(data)
      }
    }

    def serializeValue(element: T): Array[Byte] = { // if the value is null, its serialized value is null as well.
      try {
        serializer.serialize(element, outputSerializer)
      }
      catch {
        case e: IOException =>
          throw new RuntimeException("Unable to serialize record", e)
      }
      var res = outputSerializer.getByteArray
      if (res.length != outputSerializer.length) {
        val n = new Array[Byte](outputSerializer.length)
        System.arraycopy(res, 0, n, 0, outputSerializer.length)
        res = n
      }
      outputSerializer.clear
      res
    }


    /**
      * Serialize the element to byte array
      * @param data the data to serialize
      * @return the data as bytearray
      */
    def serialize(data: T): Array[Byte] = serializeInternal(data)
  }

  /**
    * Serialise the given object to byte array
    */
  object GenericSerialiser {
      def apply[TData: ClassTag](
          tData: TData)(implicit executionConfig: ExecutionConfig): Array[Byte] =
        new GenericSerialiser[TData]()(implicitly[ClassTag[TData]],executionConfig).serialize(tData)

    }

