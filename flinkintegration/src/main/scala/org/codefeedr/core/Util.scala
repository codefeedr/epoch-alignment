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

package org.codefeedr.core

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util.UUID

/**
  * Created by Niels on 02/08/2017.
  */
object Util {
  def uuidToByteArray(uuid: UUID): Array[Byte] = {
    org.apache.commons.lang3.Conversion.uuidToByteArray(uuid, new Array[Byte](16), 0, 16)
  }

  def serialize(array: Array[Any]): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(array)
    oos.close()
    stream.toByteArray
  }

}
