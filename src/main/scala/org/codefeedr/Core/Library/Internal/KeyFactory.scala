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

package org.codefeedr.Core.Library.Internal

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util.UUID

import org.codefeedr.Core.Util
import org.codefeedr.Model.{Record, Source, SubjectType}

import scala.language.postfixOps

/**
  * This class is not thread safe or serializable.
  * Should be created lazily on needed environments, and a new instance for every thread
  * Created by Niels on 23/07/2017.
  */
class KeyFactory(typeInfo: SubjectType, sinkUuid: UUID) {
  private var Sequence: Long = 0
  private var uuid = Util.UuidToByteArray(sinkUuid)

  /**
    * Set of indices that contain id fields
    */
  private val idIndices = (for (i <- typeInfo.properties.indices)
    yield if (typeInfo.properties(i).id) i else -1).filter(o => o >= 0).toSet

  /**
    * Get a unique identifier using the id fields defined in the type
    * If no id fields are defined, return an auto generated id
    * @return A bytearray as key
    */
  val GetKey: (Record) => Source = {
    if (typeInfo.properties.exists(o => o.id)) {
      GetIndexKey
    } else { _: Record =>
      GetIncrementalKey()
    }
  }

  /**
    * Returns an incremental auto generated key every time the method is called
    * @return
    */
  private def GetIncrementalKey(): Source = {
    val sequence = Sequence
    Sequence += 1
    Source(uuid, Array(sequence.toByte))
  }

  /**
    * Generates a key based on the indices defined on the record
    * @param record The record to get the key from
    * @return The key
    */
  private def GetIndexKey(record: Record): Source = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(idIndices.map(record.field))
    oos.close()
    Source(uuid, stream.toByteArray)
  }
}
