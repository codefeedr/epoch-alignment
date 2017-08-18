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

package org.codefeedr.Core.Engine.Query

import java.util.UUID

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.codefeedr.Core.Library.Internal.RecordUtils
import org.codefeedr.Core.Util
import org.codefeedr.Model._

import scala.collection.immutable.HashMap
import scala.collection.{TraversableOnce, immutable}

case class JoinState(left: immutable.HashMap[RecordSourceTrail, Record],
                     right: immutable.HashMap[RecordSourceTrail, Record])

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
    * Note that the type is not yet registered in the subjectLibrary after this method is called
    * @param leftType subject type of the left source
    * @param rightType subject type of the right source
    * @param selectLeft properties from left type that should be selected
    * @param selectRight properties from right type that should be selected
    * @param alias Name of the type created
    * @return
    */
  def buildComposedType(leftType: SubjectType,
                        rightType: SubjectType,
                        selectLeft: Array[String],
                        selectRight: Array[String],
                        alias: String): SubjectType = {
    if (selectLeft.union(selectRight).length != selectLeft.union(selectRight).distinct.length) {
      throw new Exception(
        "A join can not select the same field twice or select a equally named field from both left and right side")
    }

    val propertiesLeft =
      new RecordUtils(leftType).getIndices(selectLeft).map(o => leftType.properties(o))
    val propertiesRight =
      new RecordUtils(rightType).getIndices(selectRight).map(o => rightType.properties(o))

    SubjectType(UUID.randomUUID().toString, alias, propertiesLeft.union(propertiesRight))
  }

  /**
    * Internal function retrieving a function to obtain the join key for one side of the join
    * Would prefer to use something better than a bytearray to string conversion here, but this is easiest for now
    * Flink only supports Tuples or POJOS for key fields
    * TODO: Change the key to something better than string
    * @param properties properties that should be represented in the join key
    * @param subjectType Type of the subject to join
    * @return
    */
  private[Query] def buildPartialKeyFunction(
      properties: Array[String],
      subjectType: SubjectType): (TrailedRecord) => String = {
    val indices = new RecordUtils(subjectType).getIndices(properties)
    (r: TrailedRecord) =>
      new String(Util.serialize(for (i <- indices) yield r.record.data(i)).map(_.toChar))

  }

  /**
    * Build a function that maps a JoinRecord to its join key, using the defined types and join object
    * @param leftType Type of the objects on the left side of the join
    * @param rightType Type of the objects on the right side of the join
    * @param join The join definition itself
    * @return
    */
  def buildKeyFunction(leftType: SubjectType,
                       rightType: SubjectType,
                       join: Join): (JoinRecord) => String = {
    val leftMapper = JoinQueryComposer.buildPartialKeyFunction(join.columnsLeft, leftType)
    val rightMapper = JoinQueryComposer.buildPartialKeyFunction(join.columnsRight, rightType)
    (data: JoinRecord) =>
      {
        data match {
          case Left(d)  => leftMapper(d)
          case Right(d) => rightMapper(d)
        }
      }
  }

  /**
    * Build a function that can merge two trailedRecords together using the defined selects in the passed join
    * @param leftType subjectType of the left record
    * @param rightType subjectType of the right record
    * @param resultType subjectType of the result of the merge function
    * @param selectLeft names of the fields that should be selected from the left object type
    * @param selectRight names of the fields that should be selected from the right object type
    * @param nodeId to be assigned identifier of this node in the query graph
    */
  def buildMergeFunction(
      leftType: SubjectType,
      rightType: SubjectType,
      resultType: SubjectType,
      selectLeft: Array[String],
      selectRight: Array[String],
      nodeId: Array[Byte]): (TrailedRecord, TrailedRecord, ActionType.Value) => TrailedRecord = {
    @transient lazy val indicesLeft = new RecordUtils(leftType).getIndices(selectLeft)
    @transient lazy val indicesRight = new RecordUtils(rightType).getIndices(selectRight)

    (left: TrailedRecord, right: TrailedRecord, actionType: ActionType.Value) =>
      {
        val newKey = ComposedSource(nodeId, Array(left.trail, right.trail))
        val newData = indicesLeft
          .map(o => left.record.data(o))
          .union(indicesRight.map(o => right.record.data(o)))
        TrailedRecord(Record(newData, resultType.uuid, actionType), newKey)
      }
  }

  /**
    * Map side inner join function
    * Assumes that the caller manages the state per join key (so a seperate state exists per unique join key)
    * Usable on flink mapwithstate on a keyed stream
    * @param record The record (left or right) to join
    * @param inputState current state for the joinkey of the record
    */
  def mapSideInnerJoin(
      mergeFunction: (TrailedRecord, TrailedRecord, ActionType.Value) => TrailedRecord)(
      record: JoinRecord,
      inputState: Option[JoinState]): (TraversableOnce[TrailedRecord], Option[JoinState]) = {

    //This can be done more efficiently, but would make the method even more unreadable
    val currentState = inputState.getOrElse(
      JoinState(new HashMap[RecordSourceTrail, Record], new HashMap[RecordSourceTrail, Record]))

    //Seperate handling for left and right
    record match {

      case Left(data) =>
        data.record.action match {
          case ActionType.Add =>
            //Update the join state by adding the new element
            val state = Some(
              JoinState(currentState.left + (data.trail -> data.record), currentState.right))
            //Joining a new left element will emit all combinations of the left element with all existing right elements
            val traversable =
              currentState.right.map(t =>
                mergeFunction(data, TrailedRecord(t._2, t._1), ActionType.Add))
            (traversable, state)
          case _ => throw new NotImplementedError()
        }

      case Right(data) =>
        data.record.action match {
          case ActionType.Add =>
            val state = Some(
              //Update the join state by adding the new element
              JoinState(currentState.left, currentState.right + (data.trail -> data.record)))
            //Joining a new right element will emit all combinations of the right element with all existing left elements
            val traversable =
              currentState.left.map(t =>
                mergeFunction(TrailedRecord(t._2, t._1), data, ActionType.Add))
            (traversable, state)
          case _ => throw new NotImplementedError()
        }
    }
  }
}

/**
  * Created by Niels on 31/07/2017.
  */
class JoinQueryComposer(leftComposer: StreamComposer,
                        rightComposer: StreamComposer,
                        subjectType: SubjectType,
                        join: Join)
    extends StreamComposer {
  override def Compose(env: StreamExecutionEnvironment): DataStream[TrailedRecord] = {
    val leftStream = leftComposer.Compose(env).map(o => Left(o).asInstanceOf[JoinRecord])
    val rightStream = rightComposer.Compose(env).map(o => Right(o).asInstanceOf[JoinRecord])
    val union = leftStream.union(rightStream)

    //Build function to obtain the key
    val keyFunction = JoinQueryComposer.buildKeyFunction(leftComposer.GetExposedType(),
                                                         rightComposer.GetExposedType(),
                                                         join)
    //Build function to merge both types
    val mergeFunction = JoinQueryComposer.buildMergeFunction(
      leftComposer.GetExposedType(),
      rightComposer.GetExposedType(),
      subjectType,
      join.SelectLeft,
      join.SelectRight,
      Util.UuidToByteArray(UUID.randomUUID()))
    val mapSideJoinFunction = JoinQueryComposer.mapSideInnerJoin(mergeFunction) _
    val keyed = union.keyBy(keyFunction)
    val mapped = keyed.flatMapWithState(mapSideJoinFunction)
    mapped
  }

  /**
    * Retrieve typeinformation of the type that is exposed by the Streamcomposer (Note that these types are not necessarily registered on kafka, as it might be an intermediate type)
    *
    * @return Typeinformation of the type exposed by the stream
    */
  override def GetExposedType(): SubjectType = subjectType
}
