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

package org.codefeedr.Engine.Query
import java.util.UUID

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.codefeedr.Library.Internal.RecordUtils
import org.codefeedr.Library.SubjectLibrary
import org.codefeedr.Model.{ComposedSource, Record, SubjectType, TrailedRecord}

import scala.collection.TraversableOnce
import scala.collection.immutable
import scala.concurrent.Future

import org.apache.flink.api.scala._

case class JoinState(left: immutable.HashMap[Array[Byte], Record],
                     right: immutable.HashMap[Array[Byte], Record])

abstract class JoinRecord(data: TrailedRecord)

case class Left(data: TrailedRecord) extends JoinRecord(data)
case class Right(data: TrailedRecord) extends JoinRecord(data)

/**
  * Companion object with static utilities
  */
object JoinQueryComposer {

  /**
    * Builds a typedefinition for the joined type
    * Called by StreamComposer before creating the JoinQueryComposer
    * Communicates with potential other instances using the subjectLibrary, for registration of the alias
    * @param leftType subject type of the left source
    * @param rightType subject type of the right source
    * @param join Definition of the join
    * @return
    */
  def buildComposedType(leftType: SubjectType,
                        rightType: SubjectType,
                        join: Join): Future[SubjectType] = {
    val propertiesLeft =
      new RecordUtils(leftType).getIndices(join.SelectLeft).map(o => leftType.properties(o))
    val propertiesRight =
      new RecordUtils(rightType).getIndices(join.SelectLeft).map(o => rightType.properties(o))
    val generatedType =
      SubjectType(UUID.randomUUID().toString, join.alias, propertiesLeft.union(propertiesRight))
    SubjectLibrary.RegisterAndAwaitType(generatedType) //Call the library. The actual returned type might differ from the passed type
  }

  def buildMergeFunction(leftType: SubjectType,
                         rightType: SubjectType,
                         resultType: SubjectType,
                         join: Join,
                         nodeId: Array[Byte]): Unit = {
    val indicesLeft = new RecordUtils(leftType).getIndices(join.SelectLeft)
    val indicesRight = new RecordUtils(rightType).getIndices(join.SelectRight)

    (left: TrailedRecord, right: TrailedRecord) =>
      {
        val newKey = ComposedSource(nodeId, List(left.trail, right.trail))
        val newRecord = List[Any]()
      }
  }

  def getJoinKeyFunction(properties: List[String],
                         subjectType: SubjectType): (TrailedRecord) => List[Any] = {
    val indices = new RecordUtils(subjectType).getIndices(properties)
    (r: TrailedRecord) =>
      for (i <- indices) yield r.record.data(i)

  }

}

/**
  * Created by Niels on 31/07/2017.
  */
class JoinQueryComposer(leftComposer: StreamComposer,
                        rightComposer: StreamComposer,
                        subjectType: SubjectType,
                        keysLeft: List[String],
                        keysRight: List[String],
                        selectLeft: List[String],
                        selectRight: List[String])
    extends StreamComposer {
  override def Compose(env: StreamExecutionEnvironment): DataStream[TrailedRecord] = {
    val leftStream = leftComposer.Compose(env).map(o => Left(o).asInstanceOf[JoinRecord])
    val rightStream = rightComposer.Compose(env).map(o => Right(o).asInstanceOf[JoinRecord])
    val union = leftStream.union(rightStream)

    //Build the keyFunction
    val leftMapper = JoinQueryComposer.getJoinKeyFunction(keysLeft, leftComposer.GetExposedType())
    val rightMapper =
      JoinQueryComposer.getJoinKeyFunction(keysRight, rightComposer.GetExposedType())
    val keyFunction = (data: JoinRecord) => {
      data match {
        case Left(d) => leftMapper(d)
        case Right(d) => rightMapper(d)
      }
    }
    val keyed = union.keyBy(keyFunction)

    val mapped = keyed.flatMapWithState(mapSideInnerJoin)

    mapped
  }

  /**
    * Map side inner join function
    * Assumes that the caller manages the state per join key (so a seperate state exists per unique join key)
    * Usable on flink mapwithstate on a keyed stream
    * @param record The record (left or right) to join
    * @param inputState current state for the joinkey of the record
    */
  def mapSideInnerJoin(
      record: JoinRecord,
      inputState: Option[JoinState]): (TraversableOnce[TrailedRecord], Option[JoinState]) = {
    throw new NotImplementedError()
  }

  /**
    * Retrieve typeinformation of the type that is exposed by the Streamcomposer (Note that these types are not necessarily registered on kafka, as it might be an intermediate type)
    *
    * @return Typeinformation of the type exposed by the stream
    */
  override def GetExposedType(): SubjectType = subjectType
}
