/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.codefeedr.Library.Internal
import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import org.codefeedr.Model.{Record, SubjectType}
import scala.language.postfixOps

/**
  * This class is not thread safe or serializable.
  * Should be created lazily on needed environments, and a new instance for every thread
  * Created by Niels on 23/07/2017.
  */
class KeyFactory(typeInfo: SubjectType) {

  /**
    * Set of indices that contain id fields
    */
  private val idIndices = (for (i <- typeInfo.properties.indices)
    yield if (typeInfo.properties(i).id) i else -1).filter(o => o >= 0).toSet

  /**
    * Get a unique identifier using the id fields defined in the type
    * @param record The record to get the key for
    * @return A bytearray as key
    */
  def GetKey(record: Record): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(idIndices.map(record.data))
    oos.close()
    stream.toByteArray
  }
}
